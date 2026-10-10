import unittest

from activity_repository import WORK_TYPE_BY_KIND, empty_work_metrics


class ActivityWorkMetricTests(unittest.TestCase):
    def test_empty_work_queue_exports_each_kind_with_zero_values(self):
        metrics = empty_work_metrics()

        self.assertEqual(len(WORK_TYPE_BY_KIND), 6)
        for kind in WORK_TYPE_BY_KIND:
            label = kind.lower()
            self.assertEqual(
                metrics[
                    f'myota_activity_jobs_total{{kind="{label}",status="queued"}}'
                ],
                0.0,
            )
            self.assertEqual(
                metrics[
                    f'myota_activity_jobs_total{{kind="{label}",status="running"}}'
                ],
                0.0,
            )
            self.assertEqual(
                metrics[
                    f'myota_activity_job_queue_age_seconds{{kind="{label}"}}'
                ],
                0.0,
            )
            self.assertEqual(
                metrics[
                    f'myota_activity_work_retries_total{{kind="{label}"}}'
                ],
                0.0,
            )


if __name__ == "__main__":
    unittest.main()
