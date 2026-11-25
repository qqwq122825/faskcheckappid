"""Local-only management regressions using temporary digest/synthetic jobs."""

import errno
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from clusterlab.controller import Controller, result_record_bucket


class ManagementRegressions(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = Controller(self.temp.name)
        # Preparation is explicit and synchronous; no real nodes or executors run.
        self.service.classifier.join()
        self.submit = patch.object(self.service.preparer, 'submit')
        self.submit.start()

    def tearDown(self):
        self.submit.stop()
        self.service.close()
        self.temp.cleanup()

    def job(self, name='synthetic.txt', rows=50):
        job = self.service.create_job(name, io.BytesIO(b'hello\n' * rows), 50,
                                      mode='digest')
        self.service.prepare(job)
        return job

    def units(self, job):
        with self.service.db.connect() as db:
            return [dict(row) for row in db.execute(
                'SELECT * FROM units WHERE job_id=? ORDER BY seq', (job,))]

    def complete(self, job, seq):
        unit = next(unit for unit in self.units(job) if unit['seq'] == seq)
        token = 'synthetic-lease-' + unit['id']
        with self.service.db.transaction() as db:
            db.execute("UPDATE units SET status='leased',node_id=?,lease_token=?,"
                       "lease_until=? WHERE id=?",
                       ('synthetic-node', token, time.time() + 120, unit['id']))
        payload = b''.join((json.dumps({
            'row': row, 'status': 'valid', 'account': f'synthetic-{row}',
            'sha256': hashlib.sha256(b'hello').hexdigest(), 'bytes': 5,
        }) + '\n').encode() for row in range(unit['first_row'],
                                             unit['first_row'] + unit['rows']))
        self.service.accept_result('synthetic-node', unit['id'], token,
                                   io.BytesIO(payload), hashlib.sha256(payload).hexdigest())
        return Path(next(saved for saved in self.units(job)
                         if saved['id'] == unit['id'])['result_path'])

    def assert_no_jobs(self):
        with self.service.db.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0], 0)

    def assert_copy_failure_cleaned(self, error):
        existing = self.service.inputs / 'other-upload.txt'
        existing.write_bytes(b'keep this upload')

        def fail_copy(source, output, length):
            output.write(b'partial upload')
            raise error

        with patch('clusterlab.controller.shutil.copyfileobj', side_effect=fail_copy):
            with self.assertRaises(type(error)) as raised:
                self.service.create_job('broken.txt', io.BytesIO(b'hello\n'), 50,
                                        mode='digest')
        self.assertIs(raised.exception, error)
        self.assertEqual(existing.read_bytes(), b'keep this upload')
        self.assertEqual(list(self.service.inputs.iterdir()), [existing])
        self.assert_no_jobs()

    def test_upload_disk_error_cleans_partial_file_and_keeps_original_exception(self):
        self.assert_copy_failure_cleaned(OSError(errno.ENOSPC, 'synthetic disk full'))

    def test_upload_interruption_cleans_partial_file_and_keeps_original_exception(self):
        self.assert_copy_failure_cleaned(KeyboardInterrupt('synthetic interruption'))

    def test_upload_permission_failure_cleans_created_file(self):
        error = PermissionError('synthetic chmod failure')
        with patch.object(Path, 'chmod', side_effect=error):
            with self.assertRaises(PermissionError) as raised:
                self.service.create_job('broken.txt', io.BytesIO(b'hello\n'), 50,
                                        mode='digest')
        self.assertIs(raised.exception, error)
        self.assertEqual(list(self.service.inputs.iterdir()), [])
        self.assert_no_jobs()

    def test_upload_database_insert_failure_cleans_file(self):
        with self.service.db.transaction() as db:
            db.execute("CREATE TRIGGER fail_job_insert BEFORE INSERT ON jobs "
                       "BEGIN SELECT RAISE(ABORT,'synthetic database failure'); END")
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'synthetic database failure'):
            self.service.create_job('broken.txt', io.BytesIO(b'hello\n'), 50, mode='digest')
        self.assertEqual(list(self.service.inputs.iterdir()), [])
        self.assert_no_jobs()

    def test_upload_database_event_failure_rolls_back_job_and_cleans_file(self):
        with self.service.db.transaction() as db:
            db.execute("CREATE TRIGGER fail_upload_event BEFORE INSERT ON events "
                       "WHEN NEW.code='job_uploaded' "
                       "BEGIN SELECT RAISE(ABORT,'synthetic event failure'); END")
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'synthetic event failure'):
            self.service.create_job('broken.txt', io.BytesIO(b'hello\n'), 50, mode='digest')
        self.assertEqual(list(self.service.inputs.iterdir()), [])
        self.assert_no_jobs()

    def test_upload_cleanup_error_does_not_mask_copy_error(self):
        error = OSError(errno.ENOSPC, 'synthetic copy failure')
        with patch('clusterlab.controller.shutil.copyfileobj', side_effect=error):
            with patch.object(Path, 'unlink', side_effect=PermissionError('synthetic cleanup')):
                with self.assertRaises(OSError) as raised:
                    self.service.create_job('broken.txt', io.BytesIO(b'hello\n'), 50,
                                            mode='digest')
        self.assertIs(raised.exception, error)
        self.assert_no_jobs()

    def test_empty_upload_still_cleans_file(self):
        with self.assertRaisesRegex(ValueError, '文件为空'):
            self.service.create_job('empty.txt', io.BytesIO(), 50, mode='digest')
        self.assertEqual(list(self.service.inputs.iterdir()), [])
        self.assert_no_jobs()

    def test_same_name_upload_preserves_existing_input(self):
        first = self.job('same.txt')
        before = self.service.inputs / 'same.txt'
        second = self.service.create_job('same.txt', io.BytesIO(b'different\n'), 50,
                                         mode='digest')
        self.assertEqual(before.read_bytes(), b'hello\n' * 50)
        with self.service.db.connect() as db:
            sources = {row['id']: row['source'] for row in db.execute(
                'SELECT id,source FROM jobs')}
        self.assertNotEqual(sources[first], sources[second])
        self.assertEqual(Path(sources[second]).read_bytes(), b'different\n')

    def test_upload_retries_exclusive_create_race_without_touching_winner(self):
        candidate = self.service.inputs / 'same.txt'
        original_open = Path.open
        raced = []

        def racing_open(path, mode='r', *args, **kwargs):
            if path == candidate and mode == 'xb' and not raced:
                raced.append(path)
                with original_open(path, 'wb') as output:
                    output.write(b'concurrent winning upload')
            return original_open(path, mode, *args, **kwargs)

        with patch.object(Path, 'open', racing_open):
            job = self.service.create_job('same.txt', io.BytesIO(b'our upload\n'), 50,
                                          mode='digest')
        with self.service.db.connect() as db:
            source = Path(db.execute('SELECT source FROM jobs WHERE id=?',
                                     (job,)).fetchone()['source'])
        self.assertNotEqual(source, candidate)
        self.assertEqual(source.read_bytes(), b'our upload\n')
        self.assertEqual(candidate.read_bytes(), b'concurrent winning upload')

    def test_delete_completed_job_removes_root_results_and_keeps_other_job(self):
        job = self.job(rows=100)
        results = [self.complete(job, seq) for seq in (1, 2)]
        other = self.job('other.txt')
        other_result = self.complete(other, 1)
        other_bytes = other_result.read_bytes()
        self.assertTrue(all(path.parent == self.service.results for path in results))
        deleted = self.service.delete_job(job)
        self.assertEqual(deleted, {'deleted': job, 'source_kept': False, 'cancelled_units': 0})
        self.assertTrue(all(not path.exists() for path in results))
        self.assertEqual(other_result.read_bytes(), other_bytes)
        self.assertFalse((self.service.inputs / 'synthetic.txt').exists())
        with self.service.db.connect() as db:
            self.assertIsNone(db.execute('SELECT id FROM jobs WHERE id=?', (job,)).fetchone())
            self.assertEqual(db.execute('SELECT COUNT(*) FROM units WHERE job_id=?',
                                        (job,)).fetchone()[0], 0)
            self.assertIsNotNone(db.execute('SELECT id FROM jobs WHERE id=?',
                                           (other,)).fetchone())

    def test_delete_completed_job_keeps_shared_source_and_other_results(self):
        job = self.job()
        result = self.complete(job, 1)
        source = self.service.inputs / 'synthetic.txt'
        other = self.service.create_job_from_file(str(source), 'shared source', 50,
                                                  mode='digest')
        self.service.prepare(other)
        other_result = self.complete(other, 1)
        self.assertTrue(self.service.delete_job(job)['source_kept'])
        self.assertFalse(result.exists())
        self.assertEqual(source.read_bytes(), b'hello\n' * 50)
        self.assertTrue(other_result.exists())
        self.assertEqual(len(self.service.read_rows(other)['rows']), 50)

    def test_delete_validates_result_boundary_before_unlink(self):
        job = self.job(rows=300)
        outside = Path(self.temp.name) / 'outside.jsonl'
        sibling = Path(self.temp.name) / 'results-sibling'
        sibling.mkdir()
        sibling_file = sibling / 'keep.jsonl'
        outside.write_bytes(b'outside sentinel')
        sibling_file.write_bytes(b'sibling sentinel')
        link = self.service.results / 'outside-link.jsonl'
        link.symlink_to(outside)
        inside = self.service.results / 'owned.jsonl'
        inside.write_bytes(b'owned result')
        paths = [outside, sibling_file, self.service.results / '..' / 'outside.jsonl',
                 link, self.service.results, inside]
        with self.service.db.transaction() as db:
            for unit, path in zip(self.units(job), paths):
                db.execute("UPDATE units SET status='completed',result_path=? WHERE id=?",
                           (str(path), unit['id']))
            db.execute("UPDATE jobs SET status='completed' WHERE id=?", (job,))
        self.service.delete_job(job)
        self.assertEqual(outside.read_bytes(), b'outside sentinel')
        self.assertEqual(sibling_file.read_bytes(), b'sibling sentinel')
        self.assertTrue(self.service.results.is_dir())
        self.assertFalse(inside.exists())

    def test_delete_keeps_result_still_referenced_by_another_job(self):
        job, other = self.job(), self.job('other.txt')
        result = self.complete(job, 1)
        with self.service.db.transaction() as db:
            db.execute("UPDATE units SET status='completed',result_path=? WHERE job_id=?",
                       (str(result), other))
        self.service.delete_job(job)
        self.assertTrue(result.exists())
        self.assertEqual(len(self.service.read_rows(other)['rows']), 50)

    def partial_job(self):
        job = self.job(rows=200)
        self.complete(job, 4)
        self.complete(job, 2)
        return job

    def test_partial_all_first_page_uses_confirmed_set_not_global_rows(self):
        page = self.service.read_rows(self.partial_job(), 0, 50)
        self.assertEqual(page['total'], 100)
        self.assertEqual([row['row'] for row in page['rows']], list(range(51, 101)))

    def test_partial_all_page_crosses_unfinished_shard_gap(self):
        page = self.service.read_rows(self.partial_job(), 49, 3)
        self.assertEqual((page['total'], page['offset']), (100, 49))
        self.assertEqual([row['row'] for row in page['rows']], [100, 151, 152])

    def test_partial_all_tail_and_out_of_range_pages(self):
        job = self.partial_job()
        for offset, expected in ((50, list(range(151, 201))), (99, [200]), (100, [])):
            with self.subTest(offset=offset):
                page = self.service.read_rows(job, offset, 50)
                self.assertEqual(page['total'], 100)
                self.assertEqual([row['row'] for row in page['rows']], expected)

    def test_sparse_hundred_million_confirmed_rows_only_open_target_result(self):
        job = self.job()
        target = self.service.results / 'last-synthetic.jsonl'
        first = 199_980_001
        target.write_text(''.join(json.dumps({'row': row, 'status': 'valid'}) + '\n'
                                  for row in range(first, first + 10_000)), encoding='utf-8')
        with self.service.db.transaction() as db:
            db.execute('DELETE FROM units WHERE job_id=?', (job,))
            db.executemany(
                'INSERT INTO units(id,job_id,seq,first_row,rows,path,checksum,status,'
                'processed,result_path,result_counts,classification_version,updated) '
                'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                [(f'synthetic-{seq}', job, seq, (seq - 1) * 20_000 + 1, 10_000,
                  'synthetic-source', '0' * 64, 'completed', 10_000,
                  str(target if seq == 10_000 else self.service.results / f'missing-{seq}.jsonl'),
                  '{"valid":10000}', self.service.RESULT_CLASSIFICATION_VERSION, time.time())
                 for seq in range(1, 10_001)])
            db.execute("UPDATE jobs SET rows=200000000,status='completed' WHERE id=?", (job,))
        opened = []
        original_open = Path.open

        def counting_open(path, *args, **kwargs):
            if path.suffix == '.jsonl':
                opened.append(path)
            return original_open(path, *args, **kwargs)

        with patch.object(Path, 'open', counting_open):
            with patch('clusterlab.controller.result_record_bucket',
                       wraps=result_record_bucket) as classify:
                page = self.service.read_rows(job, 99_999_950, 50)
        self.assertEqual(page['total'], 100_000_000)
        self.assertEqual([row['row'] for row in page['rows']],
                         list(range(first + 9950, first + 10_000)))
        self.assertEqual(opened, [target])
        self.assertEqual(classify.call_count, 10_000)

    def test_missing_selected_result_is_explicit_not_a_successful_empty_page(self):
        job = self.job(rows=100)
        missing = self.complete(job, 1)
        self.complete(job, 2)
        missing.unlink()
        with self.assertRaises(FileNotFoundError):
            self.service.read_rows(job, 0, 1)
        with self.service.db.connect() as db:
            self.assertEqual(db.execute("SELECT SUM(rows) FROM units WHERE job_id=? "
                                        "AND status='completed'", (job,)).fetchone()[0], 100)
        page = self.service.read_rows(job, 50, 1)
        self.assertEqual((page['total'], page['rows'][0]['row']), (100, 51))

    def test_missing_result_reference_is_explicit(self):
        job = self.job()
        with self.service.db.transaction() as db:
            db.execute("UPDATE units SET status='completed',result_path=NULL WHERE job_id=?",
                       (job,))
        with self.assertRaises(FileNotFoundError):
            self.service.read_rows(job, 0, 1)

    def test_live_search_and_category_routes_keep_current_contract(self):
        job = self.partial_job()
        first = self.units(job)[0]
        with self.service.db.transaction() as db:
            db.execute("UPDATE units SET status='running',reported_rows=2 WHERE id=?",
                       (first['id'],))
            for row, status in ((1, 'valid'), (2, 'unknown')):
                payload = json.dumps({'row': row, 'status': status,
                                      'account': f'synthetic-{row}'})
                db.execute('INSERT INTO live_result_rows(unit_id,job_id,row,category,payload) '
                           'VALUES(?,?,?,?,?)', (first['id'], job, row, status, payload))
        page = self.service.read_rows(job, 1, 3)
        self.assertEqual((page['total'], [row['row'] for row in page['rows']]),
                         (102, [2, 51, 52]))
        page = self.service.read_rows(job, 1, 2, 'valid')
        self.assertEqual((page['total'], [row['row'] for row in page['rows']]),
                         (101, [51, 52]))
        page = self.service.read_rows(job, 2, 2, 'all', 'synthetic-15')
        self.assertEqual((page['total'], [row['row'] for row in page['rows']]),
                         (9, [153, 154]))


if __name__ == '__main__':
    unittest.main()
