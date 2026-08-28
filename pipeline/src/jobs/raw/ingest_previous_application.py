#!/usr/bin/env python3
import argparse, os, sys

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_raw_ingest import BaseRawIngestJob

def main():
    parser = argparse.ArgumentParser(description="Ingest previous_application to HDFS raw")
    parser.add_argument("--input-dir", type=str, default="/data/home-credit-default-risk")
    parser.add_argument("--output-dir", type=str, default="/raw/credit_risk")
    
    args = parser.parse_args()
    
    BaseRawIngestJob(table_name="previous_application", 
                    file_name="previous_application.csv", 
                    primary_key="SK_ID_PREV", 
                    input_base_dir=args.input_dir, 
                    output_base_dir=args.output_dir) \
                .run()

if __name__ == "__main__":
    main()
