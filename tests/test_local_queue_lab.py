import unittest
from tools.local_queue_lab import benchmark


class LocalQueueLabTests(unittest.TestCase):
    def test_results_complete_and_wait_stages_are_measured_without_network(self):
        result=benchmark(tasks=24,processes=2,threads=3,work_ms=2,resource_slots=2)
        self.assertEqual(result['configured_capacity'],6)
        self.assertEqual(result['duplicate_or_missing_results'],0)
        self.assertEqual(result['network_requests'],0)
        self.assertEqual(result['simulated_resource_slots'],2)
        self.assertGreater(result['stages']['end_to_end']['mean_ms'],0)
        self.assertGreaterEqual(result['stages']['resource_wait']['mean_ms'],0)

    def test_invalid_configuration_is_rejected_before_starting(self):
        with self.assertRaises(ValueError):benchmark(tasks=0)
        with self.assertRaises(ValueError):benchmark(work_ms=float('nan'))


if __name__=='__main__':unittest.main()
