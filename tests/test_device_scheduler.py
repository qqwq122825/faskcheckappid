import unittest

from fastcheck.device_scheduler import DeviceScheduler


class DeviceSchedulerTests(unittest.TestCase):
    def scheduler(self, slots=(1,2,3), **kwargs):
        self.now=100.0
        self.events=[]
        scheduler=DeviceScheduler(clock=lambda:self.now,log=lambda event,**fields:self.events.append(dict(fields,event=event)),**kwargs)
        for slot in slots:
            scheduler.add(slot)
        return scheduler

    def test_idle_devices_rotate_by_usage_then_oldest_assignment(self):
        scheduler=self.scheduler(rest_after=0)
        chosen=[]
        for _ in range(6):
            slot=scheduler.reserve({1,2,3})
            chosen.append(slot)
            scheduler.dispatched(slot)
            scheduler.release(slot)
            self.now+=1
        self.assertEqual(chosen,[1,2,3,1,2,3])
        self.assertEqual([v['total_uses'] for v in scheduler.report().values()],[2,2,2])

    def test_reservation_is_exclusive_and_does_not_count_as_an_attempt(self):
        scheduler=self.scheduler(slots=(1,2),rest_after=0)
        self.assertEqual(scheduler.reserve({1,2}),1)
        self.assertEqual(scheduler.reserve({1,2}),2)
        self.assertIsNone(scheduler.reserve({1,2}))
        self.assertEqual(scheduler.report()['1']['total_uses'],0)
        scheduler.release(1)
        self.assertEqual(scheduler.reserve({1,2}),1)

    def test_rest_is_staggered_and_reuses_the_same_slot_after_expiry(self):
        scheduler=self.scheduler(rest_after=1,rest_seconds=5,rest_slots=1,minimum_ready=2)
        first=scheduler.reserve({1,2,3})
        scheduler.dispatched(first)
        scheduler.release(first)
        self.assertEqual(scheduler.snapshot()['resting'],1)
        self.assertEqual(scheduler.snapshot()['ready'],2)
        second=scheduler.reserve({1,2,3})
        self.assertNotEqual(second,first)
        scheduler.dispatched(second)
        scheduler.release(second)
        self.assertEqual(scheduler.snapshot()['resting'],1)
        self.now+=5
        self.assertEqual(scheduler.snapshot()['resting'],0)
        third=scheduler.reserve({1,2,3})
        self.assertEqual(third,3)
        self.assertEqual(scheduler.report()[str(first)]['rest_count'],1)
        self.assertEqual(scheduler.report()[str(second)]['rest_count'],1)
        self.assertTrue(any(e['event']=='device_rest_ended' for e in self.events))

    def test_two_ready_devices_defer_rest_until_a_spare_arrives(self):
        scheduler=self.scheduler(slots=(1,2),rest_after=1,rest_seconds=5,minimum_ready=2)
        for _ in range(2):
            slot=scheduler.reserve({1,2})
            scheduler.dispatched(slot)
            scheduler.release(slot)
            self.now+=1
        self.assertEqual(scheduler.snapshot()['resting'],0)
        self.assertEqual(scheduler.snapshot()['ready'],2)
        scheduler.add(3)
        self.assertEqual(scheduler.reserve({1,2,3}),3)
        self.assertEqual(scheduler.snapshot()['resting'],1)
        self.assertEqual(scheduler.snapshot()['ready'],2)

    def test_externally_blocked_devices_are_not_used_to_satisfy_rest_floor(self):
        scheduler=self.scheduler(rest_after=1,rest_seconds=5,minimum_ready=2)
        slot=scheduler.reserve({2})
        self.assertEqual(slot,2)
        scheduler.dispatched(slot)
        scheduler.release(slot)
        self.assertEqual(scheduler.snapshot()['ready'],1)
        self.assertEqual(scheduler.snapshot()['resting'],0)
        self.assertEqual(scheduler.reserve({2}),2)

    def test_configurable_shared_slots_balance_across_two_devices(self):
        scheduler=self.scheduler(slots=(1,2),concurrency=3,rest_after=0)
        chosen=[]
        for _ in range(6):
            slot=scheduler.reserve({1,2});chosen.append(slot);scheduler.dispatched(slot)
        self.assertEqual(chosen.count(1),3)
        self.assertEqual(chosen.count(2),3)
        self.assertIsNone(scheduler.reserve({1,2}))
        scheduler.release(1)
        self.assertEqual(scheduler.reserve({1,2}),1)


if __name__=='__main__':
    unittest.main()
