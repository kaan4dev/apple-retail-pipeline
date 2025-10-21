import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
from src.utils.io_datalake import write_to_datalake

load_dotenv()

CURRENT_DIR = os.getcwd()
PARQUET_PATH = os.path.join(
    CURRENT_DIR,
    "data/processed/apple/processed_sales.parquet"
)

if os.path.exists(PARQUET_PATH):
    print(f"Uploading file: {PARQUET_PATH}")
    remote_path = "processed/apple/processed_sales.parquet"
    write_to_datalake(PARQUET_PATH, remote_path)
    print("Upload completed successfully.")
else:
    print("Processed file not found. Run transform.py first.")
