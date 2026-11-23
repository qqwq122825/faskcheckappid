import unittest

from clusterlab.refill import IdempotentTaskLedger, TaskRefillBuffer


class RefillSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.ledger = IdempotentTaskLedger()
        self.ledger.add((f'{index:05d}', f'account-{index:05d}')
                        for index in range(10000))

    def tearDown(self):
        self.ledger.close()

    def refill(self, buffer, node='node-a', now=1000):
        request = buffer.plan_refill()
        if request is None:
            return 0, False
        tasks = self.ledger.lease(node, request.request_id, request.count,
                                  now=now, lease_seconds=100000)
        added, immediate = buffer.accept(request.request_id, tasks, now=now)
        return added, immediate

    def test_response_loss_reuses_request_id_without_double_leasing(self):
        buffer = TaskRefillBuffer(100, 300)
        request = buffer.plan_refill()
        first = self.ledger.lease('node-a', request.request_id, request.count,
                                  now=1000, lease_seconds=100000)
        retry = buffer.plan_refill()
        second = self.ledger.lease('node-a', retry.request_id, retry.count,
                                   now=1001, lease_seconds=100000)
        self.assertEqual(retry.request_id, request.request_id)
        self.assertEqual(second, first)
        added, _ = buffer.accept(request.request_id, second, now=1001)
        self.assertEqual((added, len(buffer)), (300, 300))

    def test_two_nodes_never_receive_the_same_current_execution(self):
        a = self.ledger.lease('node-a', 'a' * 32, 300, now=1000)
        b = self.ledger.lease('node-b', 'b' * 32, 300, now=1000)
        self.assertFalse({item['task_id'] for item in a}
                         & {item['task_id'] for item in b})

    def test_invalid_response_does_not_partially_enter_local_queue(self):
        buffer = TaskRefillBuffer(100, 300)
        request = buffer.plan_refill()
        tasks = self.ledger.lease('node-a', request.request_id, 2,
                                  now=1000, lease_seconds=100000)
        tasks[1] = dict(tasks[1], version='bad')
        with self.assertRaises(ValueError):
            buffer.accept(request.request_id, tasks, now=1000)
        self.assertEqual(len(buffer), 0)
        self.assertEqual(buffer.plan_refill().request_id, request.request_id)

    def test_workers_keep_draining_while_refill_request_is_in_flight(self):
        buffer = TaskRefillBuffer(100, 300)
        first = buffer.plan_refill()
        tasks = self.ledger.lease('node-a', first.request_id, first.count,
                                  now=1000, lease_seconds=100000)
        buffer.accept(first.request_id, tasks, now=1000)
        for _ in range(200):
            self.assertIsNotNone(buffer.take(now=1000))
        self.assertEqual(len(buffer), 100)
        refill = buffer.plan_refill()
        self.assertEqual(refill.count, 200)
        # The request is in flight; workers consume forty more tasks.
        for _ in range(40):
            self.assertIsNotNone(buffer.take(now=1000))
        self.assertEqual(len(buffer), 60)
        tasks = self.ledger.lease('node-a', refill.request_id, refill.count,
                                  now=1000, lease_seconds=100000)
        buffer.accept(refill.request_id, tasks, now=1000)
        self.assertEqual(len(buffer), 260)

    def test_one_stuck_slot_does_not_stop_refill_or_other_work(self):
        buffer = TaskRefillBuffer(100, 300)
        refills = 0
        exhausted = False

        def top_up():
            nonlocal refills, exhausted
            while not exhausted:
                request = buffer.plan_refill()
                if request is None:
                    return
                tasks = self.ledger.lease('node-a', request.request_id,
                                          request.count, now=1000,
                                          lease_seconds=100000)
                _added, immediate = buffer.accept(request.request_id, tasks, now=1000)
                refills += 1
                if not tasks:
                    exhausted = True
                    return
                if not immediate:
                    return

        # Startup: idle execution slots drain the queue while the independent
        # refiller keeps only 100..300 waiting tasks, never counting running.
        running = []
        while len(running) < 1000:
            top_up()
            task = buffer.take(now=1000)
            self.assertIsNotNone(task)
            running.append(task)
        blocked = running.pop(0)

        completed_before_blocked = 0
        while running or len(buffer) or not exhausted:
            if not running:
                top_up()
                task = buffer.take(now=1000)
                if task is None:
                    break
                running.append(task)
            current = running.pop(0)
            self.assertTrue(self.ledger.complete(
                'node-a', current['task_id'], current['version'], 'ok', now=1001))
            completed_before_blocked += 1
            top_up()
            replacement = buffer.take(now=1000)
            if replacement is not None:
                running.append(replacement)

        self.assertGreater(completed_before_blocked, 1000)
        self.assertGreater(refills, 2)
        self.assertEqual(self.ledger.task(blocked['task_id'])['status'], 'leased')
        self.assertTrue(self.ledger.complete(
            'node-a', blocked['task_id'], blocked['version'], 'ok', now=1002))
        self.assertEqual(self.ledger.counts(), {'completed': 10000})

    def test_expired_lease_gets_new_version_and_rejects_old_result(self):
        first = self.ledger.lease('node-a', 'c' * 32, 1,
                                  now=1000, lease_seconds=10)[0]
        second = self.ledger.lease('node-b', 'd' * 32, 1,
                                   now=1011, lease_seconds=10)[0]
        self.assertEqual(second['task_id'], first['task_id'])
        self.assertEqual(second['version'], first['version'] + 1)
        self.assertFalse(self.ledger.complete(
            'node-a', first['task_id'], first['version'], 'late', now=1012))
        self.assertTrue(self.ledger.complete(
            'node-b', second['task_id'], second['version'], 'ok', now=1012))


if __name__ == '__main__':
    unittest.main()
