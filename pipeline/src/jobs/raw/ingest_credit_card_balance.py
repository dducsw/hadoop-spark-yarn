#!/usr/bin/env python3
import argparse, os, sys

JOB_DIR = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DIR = os.path.abspath(os.path.join(JOB_DIR, "..", "..", ".."))
SRC_DIR = os.path.abspath(os.path.join(JOB_DIR, "..", ".."))
sys.path.extend([PIPELINE_DIR, SRC_DIR])

from src.common.base_raw_ingest import BaseRawIngestJob, LoadType, SourceType


def main():
    parser = argparse.ArgumentParser(description="Ingest credit_card_balance (Incremental) from DB or CSV")
    parser.add_argument("--source-type", type=str, default="db", choices=["db", "csv"])
    parser.add_argument("--connection-string", type=str, default=None, help="e.g. postgresql://user:pwd@host:port/db")
    parser.add_argument("--input-dir", type=str, default="/data/home-credit-default-risk")
    parser.add_argument("--output-dir", type=str, default="/raw/credit_risk")

    args = parser.parse_args()

    BaseRawIngestJob(
        table_name="credit_card_balance",
        primary_key=None,
        source_type=SourceType(args.source_type),
        connection_string=args.connection_string,
        load_type=LoadType.INCREMENTAL,
        watermark_col="updated_at",
        input_base_dir=args.input_dir,
        output_base_dir=args.output_dir,
    ).run()


if __name__ == "__main__":
    main()
