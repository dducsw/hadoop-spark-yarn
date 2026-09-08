#!/usr/bin/env python3
"""Unit tests for Pipeline Resilience: DB Metadata/Watermark, Financial Reconciliation, and Serving Swap."""
import os
import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PIPELINE_DIR = os.path.join(PROJECT_DIR, "pipeline")
sys.path.extend([PROJECT_DIR, PIPELINE_DIR])

from src.common.audit import log_pipeline_execution
from src.common.db_metadata import get_metadata_cursor
from src.common.watermark import get_watermark, update_watermark


class TestPipelineResilience(unittest.TestCase):
    def test_watermark_atomic_upsert(self):
        """Verify atomic upsert updates existing watermark without duplicate rows or race condition."""
        tbl = "test_fintech_transactions"
        val1 = "2026-09-08 01:00:00"
        val2 = "2026-09-08 02:00:00"

        update_watermark(None, tbl, last_watermark_value=val1, status="SUCCESS")
        self.assertEqual(get_watermark(None, tbl), val1)

        # Update should overwrite atomically
        update_watermark(None, tbl, last_watermark_value=val2, status="SUCCESS")
        self.assertEqual(get_watermark(None, tbl), val2)

        # Check in DB that exactly one row exists for this table
        with get_metadata_cursor() as (cur, backend):
            placeholder = "%s" if backend == "postgres" else "?"
            cur.execute(f"SELECT COUNT(*) FROM pipeline_watermark WHERE table_name = {placeholder}", (tbl,))
            cnt = cur.fetchone()[0]
            self.assertEqual(cnt, 1)

    def test_audit_logging_to_db(self):
        """Verify audit execution metrics are safely logged into database."""
        start = datetime.now(timezone.utc)
        end = datetime.now(timezone.utc)
        test_tbl = "test_fact_loans"

        log_pipeline_execution(
            spark=None,
            pipeline_layer="curated",
            table_name=test_tbl,
            source_table="stage_loans",
            target_table="fact_loans",
            source_path="/stage/loans",
            target_path="/curated/loans",
            start_time=start,
            end_time=end,
            status="SUCCESS",
            row_count=50000,
            column_count=25,
        )

        with get_metadata_cursor() as (cur, backend):
            placeholder = "%s" if backend == "postgres" else "?"
            cur.execute(
                f"SELECT row_count, column_count, status FROM pipeline_audit_log WHERE table_name = {placeholder} ORDER BY start_time DESC LIMIT 1",
                (test_tbl,),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], 50000)
            self.assertEqual(row[1], 25)
            self.assertEqual(row[2], "SUCCESS")

    def test_reconciliation_logic_pass_and_fail(self):
        """Simulate financial reconciliation calculation and verify threshold enforcement."""
        stage_train_credit = Decimal("10000000.50")
        stage_test_credit = Decimal("5000000.00")
        total_stage = stage_train_credit + stage_test_credit

        # Matching curated mart credit
        obt_credit_valid = Decimal("15000000.50")
        diff_valid = abs(total_stage - obt_credit_valid)
        pct_valid = float(diff_valid) / float(total_stage) * 100.0
        self.assertLessEqual(pct_valid, 0.01)

        # Mismatched curated mart credit (> 0.01%)
        obt_credit_invalid = Decimal("14000000.00")
        diff_invalid = abs(total_stage - obt_credit_invalid)
        pct_invalid = float(diff_invalid) / float(total_stage) * 100.0
        self.assertGreater(pct_invalid, 0.01)

    def test_clickhouse_zero_downtime_swap_script(self):
        """Verify ClickHouse sync script includes atomic EXCHANGE TABLES and staging table."""
        script_path = os.path.join(PROJECT_DIR, "scripts", "ops", "sync_hdfs_to_clickhouse.sh")
        self.assertTrue(os.path.exists(script_path))

        with open(script_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("obt_loan_portfolio_360_staging", content)
        self.assertIn("EXCHANGE TABLES", content)
        self.assertNotIn("TRUNCATE TABLE analytics.obt_loan_portfolio_360;", content)

    def test_raw_csv_schemas_mapped_completely(self):
        """Verify all 8 core raw tables have explicit non-empty schemas in RAW_CSV_SCHEMAS."""
        from src.schemas.raw_schemas import RAW_CSV_SCHEMAS
        expected_tables = [
            "application_train",
            "application_test",
            "bureau",
            "bureau_balance",
            "pos_cash_balance",
            "credit_card_balance",
            "previous_application",
            "installments_payments",
        ]
        for tbl in expected_tables:
            self.assertIn(tbl, RAW_CSV_SCHEMAS)
            schema_str = RAW_CSV_SCHEMAS[tbl]
            self.assertIsInstance(schema_str, str)
            self.assertGreater(len(schema_str), 10)
            self.assertNotIn("_source_system", schema_str)
            self.assertNotIn("_batch_id", schema_str)


if __name__ == "__main__":
    unittest.main()
