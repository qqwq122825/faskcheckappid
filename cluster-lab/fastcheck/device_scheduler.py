"""Parent-owned device leases, fair selection and staggered workload rests.

Rest is local scheduling policy, never a conclusion about Apple's restrictions.
No authentication result or error code is an input to this scheduler.
"""
from dataclasses import dataclass
import threading
import time
import random


@dataclass
class DeviceState:
    in_flight: int = 0
    total_uses: int = 0
    cycle_uses: int = 0
    last_used: float = 0
    rest_until: float = 0
    rest_count: int = 0
    deferred_logged: bool = False
    draining: bool = False

    @property
    def busy(self):
        return self.in_flight > 0


class DeviceScheduler:
    def __init__(self, *, rest_after=20, rest_seconds=30, rest_slots=1,
                 minimum_ready=2, concurrency=1, selection='balanced', chooser=None, clock=time.monotonic, log=None):
        self.rest_after, self.rest_seconds = rest_after, rest_seconds
        self.rest_slots, self.minimum_ready = rest_slots, minimum_ready
        self.concurrency = concurrency
        self.selection,self.chooser=selection,chooser or random.SystemRandom()
        self.clock, self.log = clock, log or (lambda *a, **k: None)
        self.lock = threading.RLock()
        self.devices, self.allowed = {}, set()

    def add(self, slot):
        with self.lock:
            self.devices.setdefault(slot, DeviceState())
            self.allowed.add(slot)

    def _wake(self):
        now = self.clock()
        for slot, state in self.devices.items():
            if state.rest_until and state.rest_until <= now:
                state.rest_until = 0
                self.log('device_rest_ended', device=slot, total_uses=state.total_uses)

    def _schedule_rests(self):
        self._wake()
        if not self.rest_after or self.rest_seconds <= 0:
            return
        resting = sum(state.rest_until > 0 or state.draining for state in self.devices.values())
        ready = sum(not self.devices[slot].rest_until and not self.devices[slot].draining for slot in self.allowed)
        due = sorted((slot for slot in self.allowed if not self.devices[slot].draining
                      and not self.devices[slot].rest_until
                      and self.devices[slot].cycle_uses >= self.rest_after),
                     key=lambda slot: (-self.devices[slot].cycle_uses,
                                       self.devices[slot].last_used, slot))
        for slot in due:
            state = self.devices[slot]
            if resting >= self.rest_slots or ready <= self.minimum_ready:
                if not state.deferred_logged:
                    self.log('device_rest_deferred', device=slot, cycle_uses=state.cycle_uses,
                             reason='preserve_ready_devices_or_rest_slot_limit')
                    state.deferred_logged = True
                continue
            used = state.cycle_uses
            state.draining = True
            state.deferred_logged = False
            resting += 1
            ready -= 1
            if state.busy:
                self.log('device_rest_draining',device=slot,in_flight=state.in_flight,cycle_uses=used)
        for slot,state in self.devices.items():
            if state.draining and not state.busy:
                used=state.cycle_uses
                state.draining=False
                state.rest_until=self.clock()+self.rest_seconds
                state.cycle_uses=0
                state.rest_count+=1
                self.log('device_rest_started',device=slot,uses_before_rest=used,
                         total_uses=state.total_uses,rest_seconds=self.rest_seconds)

    def reserve(self, allowed):
        with self.lock:
            self.allowed = set(allowed) & self.devices.keys()
            self._schedule_rests()
            candidates = [slot for slot in self.allowed if self.devices[slot].in_flight < self.concurrency
                          and not self.devices[slot].rest_until and not self.devices[slot].draining]
            if not candidates:
                return None
            if self.selection=='prefer_new':
                minimum=min(self.devices[value].in_flight for value in candidates)
                choices=[value for value in sorted(candidates) if self.devices[value].in_flight<=minimum+1]
                order={value:index+1 for index,value in enumerate(self.devices)}
                slot=self.chooser.choices(choices,weights=[order[value] for value in choices],k=1)[0]
            else:
                slot = min(candidates, key=lambda value: (self.devices[value].in_flight,self.devices[value].total_uses,
                                                          self.devices[value].last_used, value))
            self.devices[slot].in_flight += 1
            return slot

    def set_allowed(self, allowed):
        with self.lock:
            self.allowed = set(allowed) & self.devices.keys()

    def dispatched(self, slot):
        with self.lock:
            state = self.devices[slot]
            if not state.busy or state.rest_until:
                raise RuntimeError('Dispatch requires an exclusively leased, non-resting device')
            state.total_uses += 1
            state.cycle_uses += 1
            state.last_used = self.clock()
            return {'device_total_uses': state.total_uses, 'device_cycle_uses': state.cycle_uses}

    def release(self, slot, *, allow_rest=True):
        with self.lock:
            state=self.devices[slot]
            if state.in_flight<=0:
                raise RuntimeError('Device lease was already released')
            state.in_flight-=1
            if allow_rest:
                self._schedule_rests()

    def snapshot(self):
        with self.lock:
            self._wake()
            ready = [slot for slot in self.allowed if not self.devices[slot].rest_until and not self.devices[slot].draining]
            return {'total': len(self.devices), 'ready': len(ready),
                    'idle': sum(not self.devices[slot].busy for slot in ready),
                    'resting': sum(state.rest_until > 0 for state in self.devices.values()),
                    'draining':sum(state.draining for state in self.devices.values()),
                    'in_flight':sum(state.in_flight for state in self.devices.values())}

    def report(self):
        with self.lock:
            self._wake()
            now = self.clock()
            return {str(slot): {'total_uses': state.total_uses, 'cycle_uses': state.cycle_uses,
                               'busy': state.busy, 'rest_count': state.rest_count,
                               'in_flight':state.in_flight,'draining':state.draining,
                               'rest_remaining_seconds': round(max(0,state.rest_until-now),2)}
                    for slot,state in sorted(self.devices.items())}
