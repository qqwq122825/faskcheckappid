"""Idempotent task leasing and a bounded node-side refill queue.

The queue counts only work waiting locally.  Running work is deliberately not
included: a stuck execution slot must never suppress background replenishment.
"""

from collections import deque
from dataclasses import dataclass
import json
import re
import sqlite3
import threading
import time
import uuid


REQUEST_ID = re.compile(r'^[0-9a-f]{32}$')


@dataclass(frozen=True)
class RefillRequest:
    request_id: str
    count: int


class TaskRefillBuffer:
    """Thread-safe low/high-water queue with one idempotent request in flight."""

    def __init__(self, low_water=100, high_water=300):
        low_water, high_water = int(low_water), int(high_water)
        if low_water < 0 or high_water <= low_water:
            raise ValueError('refill watermarks must satisfy 0 <= low < high')
        self.low_water = low_water
        self.high_water = high_water
        self._queue = deque()
        self._seen = set()
        self._request = None
        self._lock = threading.Lock()

    def __len__(self):
        with self._lock:
            return len(self._queue)

    @property
    def inflight(self):
        with self._lock:
            return self._request

    def plan_refill(self):
        """Return the current request for retry, or create one below low water."""
        with self._lock:
            if self._request is not None:
                return self._request
            queued = len(self._queue)
            if queued > self.low_water:
                return None
            self._request = RefillRequest(uuid.uuid4().hex,
                                          self.high_water - queued)
            return self._request

    def accept(self, request_id, tasks, now=None):
        """Atomically enqueue one response and report whether to check again."""
        now = time.time() if now is None else float(now)
        with self._lock:
            if self._request is None or request_id != self._request.request_id:
                raise ValueError('refill response does not match the in-flight request')
            prepared, response_keys = [], set()
            for task in tasks or ():
                task_id = str(task.get('task_id') or '')
                version = task.get('version')
                lease_until = task.get('lease_until')
                if (not task_id or type(version) is not int or version < 1
                        or not isinstance(lease_until, (int, float))):
                    raise ValueError('invalid leased task')
                if float(lease_until) <= now:
                    continue
                key = (task_id, version)
                if key in self._seen or key in response_keys:
                    continue
                response_keys.add(key)
                prepared.append((key, dict(task)))
            # Validate the complete response before mutating the queue.  A bad
            # item can therefore be retried with the same request_id without a
            # partially accepted prefix.
            for key, task in prepared:
                self._seen.add(key)
                self._queue.append(task)
            self._request = None
            return len(prepared), len(self._queue) <= self.low_water

    def take(self, now=None):
        """Take one unexpired task; running tasks no longer count as queued."""
        now = time.time() if now is None else float(now)
        with self._lock:
            while self._queue:
                task = self._queue.popleft()
                if float(task['lease_until']) > now:
                    return task
            return None


LEASE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS refill_tasks (
 task_id TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
 owner TEXT, version INTEGER NOT NULL DEFAULT 0, lease_until REAL,
 result TEXT, updated REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS refill_tasks_queue ON refill_tasks(status,task_id);
CREATE TABLE IF NOT EXISTS refill_requests (
 node_id TEXT NOT NULL, request_id TEXT NOT NULL, response TEXT NOT NULL,
 created REAL NOT NULL, PRIMARY KEY(node_id,request_id)
);
'''


class IdempotentTaskLedger:
    """Small SQLite-backed reference implementation for controller batch leases."""

    def __init__(self, path=':memory:'):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(LEASE_SCHEMA)
        self._lock = threading.Lock()

    def close(self):
        self.db.close()

    def add(self, tasks, now=None):
        now = time.time() if now is None else float(now)
        with self._lock, self.db:
            self.db.executemany(
                'INSERT INTO refill_tasks(task_id,payload,updated) VALUES(?,?,?)',
                ((str(task_id), str(payload), now) for task_id, payload in tasks))

    def lease(self, node_id, request_id, count, now=None, lease_seconds=120):
        """Select and mark tasks in one transaction; duplicate IDs replay exactly."""
        if not REQUEST_ID.fullmatch(str(request_id or '')):
            raise ValueError('invalid refill request id')
        count = int(count)
        if count < 1 or count > 300:
            raise ValueError('refill count must be between 1 and 300')
        now = time.time() if now is None else float(now)
        lease_until = now + max(1.0, float(lease_seconds))
        with self._lock, self.db:
            saved = self.db.execute(
                'SELECT response FROM refill_requests WHERE node_id=? AND request_id=?',
                (node_id, request_id)).fetchone()
            if saved:
                return json.loads(saved['response'])
            self.db.execute(
                "UPDATE refill_tasks SET status='pending',owner=NULL,lease_until=NULL,updated=? "
                "WHERE status='leased' AND lease_until<=?", (now, now))
            rows = list(self.db.execute(
                "SELECT task_id,payload,version FROM refill_tasks WHERE status='pending' "
                "ORDER BY task_id LIMIT ?", (count,)))
            response = []
            for row in rows:
                version = int(row['version']) + 1
                changed = self.db.execute(
                    "UPDATE refill_tasks SET status='leased',owner=?,version=?,lease_until=?,updated=? "
                    "WHERE task_id=? AND status='pending'",
                    (node_id, version, lease_until, now, row['task_id'])).rowcount
                if changed:
                    response.append({'task_id': row['task_id'], 'version': version,
                                     'lease_until': lease_until,
                                     'payload': row['payload']})
            encoded = json.dumps(response, separators=(',', ':'), sort_keys=True)
            self.db.execute(
                'INSERT INTO refill_requests(node_id,request_id,response,created) VALUES(?,?,?,?)',
                (node_id, request_id, encoded, now))
            return json.loads(encoded)

    def complete(self, node_id, task_id, version, result='', now=None):
        """Accept only the current execution version; identical replay is idempotent."""
        now = time.time() if now is None else float(now)
        encoded = str(result)
        with self._lock, self.db:
            row = self.db.execute(
                'SELECT status,owner,version,result FROM refill_tasks WHERE task_id=?',
                (task_id,)).fetchone()
            if not row or row['owner'] != node_id or int(row['version']) != int(version):
                return False
            if row['status'] == 'completed':
                return row['result'] == encoded
            if row['status'] != 'leased':
                return False
            self.db.execute(
                "UPDATE refill_tasks SET status='completed',result=?,updated=? WHERE task_id=?",
                (encoded, now, task_id))
            return True

    def counts(self):
        with self._lock:
            return {row['status']: int(row['n']) for row in self.db.execute(
                'SELECT status,COUNT(*) AS n FROM refill_tasks GROUP BY status')}

    def task(self, task_id):
        with self._lock:
            row = self.db.execute('SELECT * FROM refill_tasks WHERE task_id=?',
                                  (task_id,)).fetchone()
            return dict(row) if row else None
