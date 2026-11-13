from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import time


def private_dir(path):
    path = Path(path).resolve()
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


def atomic_json(path, value):
    path = Path(path)
    temp = path.with_name(path.name + '.' + secrets.token_hex(5) + '.tmp')
    try:
        with temp.open('x', encoding='utf-8') as stream:
            temp.chmod(0o600)
            json.dump(value, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text('utf-8'))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


class Database:
    def __init__(self, path, schema):
        self.path = Path(path)
        with self.connect() as db:
            db.executescript(schema)
        self.path.chmod(0o600)

    def connect(self):
        db = sqlite3.connect(self.path, timeout=20, isolation_level=None, factory=ClosingConnection)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA busy_timeout=20000')
        return db

    @contextmanager
    def transaction(self):
        db = self.connect()
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()


def event(db, code, message, job_id=None, node_id=None):
    db.execute('INSERT INTO events(at,code,message,job_id,node_id) VALUES(?,?,?,?,?)',
               (time.time(), code, message, job_id, node_id))


def integer(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f'{name}应为 {low}–{high} 的整数')
    return value
