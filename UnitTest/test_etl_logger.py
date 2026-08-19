import unittest
from src.helpers.logger import ETLLogger, JobMetrics


class TestETLLogger(unittest.TestCase):

    def test_etl_logger_success_flow(self):
        logger_inst = ETLLogger(
            job_id="customer_load",
            job_name="Customer Load",
            source="BANK.CUSTOMER",
            target="hive.edw_bronze.customer",
            load_type="full",
        )

        self.assertTrue(logger_inst.run_id.startswith("run_"))

        logger_inst.record_rows_read(1250)
        logger_inst.record_rows_written(1250)

        metrics = logger_inst.complete_success()

        self.assertIsInstance(metrics, JobMetrics)
        self.assertEqual(metrics.job_id, "customer_load")
        self.assertEqual(metrics.job_name, "Customer Load")
        self.assertEqual(metrics.status, "SUCCESS")
        self.assertEqual(metrics.rows_read, 1250)
        self.assertEqual(metrics.rows_written, 1250)
        self.assertIsNotNone(metrics.end_time)
        self.assertIsNone(metrics.error_message)
        self.assertGreaterEqual(metrics.duration, 0.0)

    def test_etl_logger_failure_flow(self):
        logger_inst = ETLLogger(
            job_id="customer_load",
            job_name="Customer Load",
            source="BANK.CUSTOMER",
            target="hive.edw_bronze.customer",
            load_type="full",
        )

        test_exception = RuntimeError("Database connection lost")
        metrics = logger_inst.complete_failure(test_exception)

        self.assertEqual(metrics.status, "FAILED")
        self.assertEqual(metrics.error_message, "Database connection lost")
        self.assertIsNotNone(metrics.end_time)

    def test_etl_logger_json_structure(self):
        logger_inst = ETLLogger(
            job_id="customer_load",
            job_name="Customer Load",
            source="BANK.CUSTOMER",
            target="hive.edw_bronze.customer",
            load_type="full",
        )
        metrics = logger_inst.complete_success(rows_read=100, rows_written=100)
        json_data = metrics.to_dict()

        expected_fields = {
            "job_id",
            "job_name",
            "run_id",
            "source",
            "target",
            "load_type",
            "start_time",
            "end_time",
            "status",
            "rows_read",
            "rows_written",
            "duration",
            "error_message",
        }

        self.assertEqual(set(json_data.keys()), expected_fields)


if __name__ == "__main__":
    unittest.main()
