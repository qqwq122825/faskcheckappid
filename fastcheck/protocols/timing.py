"""Optional phase timing; never includes request bodies or credentials."""
from contextlib import contextmanager
from itertools import count
import threading
import time

_phase_ids = count(1)


def exception_metadata(exc):
    """Exception classes and numeric OS codes only; never str(exc) or URLs."""
    pending, seen, names, codes = [exc], set(), [], set()
    while pending and len(seen) < 16:
        value = pending.pop()
        if not isinstance(value, BaseException) or id(value) in seen:
            continue
        seen.add(id(value))
        names.append(type(value).__name__)
        for key in ("errno", "winerror"):
            code = getattr(value, key, None)
            if type(code) is int:
                codes.add(code)
        pending.extend([value.__cause__, value.__context__, *value.args])
        pending.append(getattr(value, "reason", None))
    return {"error_type": type(exc).__name__, "error_chain": names, "os_errors": sorted(codes)}


def emit(callback, event):
    if callback is not None:
        try:
            callback(event)
        except Exception:
            pass  # Diagnostics must not change authentication behavior.


@contextmanager
def phase(callback, name):
    metadata = {}
    if callback is None:
        yield metadata
        return
    started, cpu_started = time.perf_counter(), time.thread_time()
    identity = {"phase_id": next(_phase_ids), "thread_id": threading.get_ident(), "clock": "perf_counter"}
    emit(callback, {"event": "phase_start", "phase": name, "at": started,
                    "started_at": started, **identity})
    try:
        yield metadata
    except BaseException as exc:
        metadata.update(exception_metadata(exc))
        raise
    finally:
        ended, cpu_ended = time.perf_counter(), time.thread_time()
        safe = {key: value for key, value in metadata.items()
                if key in ("http", "apple_ec", "error_type", "error_chain", "os_errors", "iterations")}
        emit(callback, {"event": "phase_end", "phase": name, "at": ended,
                        "started_at": started, "ended_at": ended, **identity,
                        "ms": round((ended-started)*1000, 3),
                        "cpu_ms": round((cpu_ended-cpu_started)*1000, 3), **safe})
