import os
import sys
import pandas as pd

def project_root() -> str:
    """Proje kökünü (apple-retail-pipeline) bul."""
    cur = os.getcwd()
    while not cur.endswith("apple-retail-pipeline") and cur != "/":
        cur = os.path.dirname(cur)
    return cur

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def warn_missing(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"missing columns for {label}: {missing}")

ROOT = project_root()
os.chdir(ROOT)
PROCESSED = os.path.join(ROOT, "data", "processed", "apple", "processed_sales.parquet")
WH_DIR = os.path.join(ROOT, "data", "warehouse", "apple")
ensure_dir(WH_DIR)

df = pd.read_parquet(PROCESSED)
print(f"processed dataframe is downloaded: {df.shape[0]}rows, {df.shape[1]} rows.")

if "sale_date" in df.columns:
 if not pd.api.types.is_datetime64_any_dtype(df["sale_date"]):
        df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")

store_cols = ["store_id", "store_name", "region"]
warn_missing(df, store_cols, "dim_store")
dim_store = df[[c for c in store_cols if c in df.columns]].drop_duplicates().reset_index(drop=True)

cat_cols = ["category_id", "category_name"]
warn_missing(df, cat_cols, "dim_category")
dim_category = df[[c for c in cat_cols if c in df.columns]].drop_duplicates().reset_index(drop=True)

prod_cols = ["product_id", "product_name", "category_id"]
warn_missing(df, prod_cols, "dim_product")
dim_product = df[[c for c in prod_cols if c in df.columns]].drop_duplicates().reset_index(drop=True)

warranty_cols = ["claim_id", "claim_date", "repair_status"]
warn_missing(df, warranty_cols, "dim_warranty")
dim_warranty = df[[c for c in warranty_cols if c in df.columns]].drop_duplicates().reset_index(drop=True)
if "claim_date" in dim_warranty.columns:
    dim_warranty["claim_date"] = pd.to_datetime(dim_warranty["claim_date"], errors="coerce")

date_cols_needed = ["sale_date"]
warn_missing(df, date_cols_needed, "dim_date")
if "sale_date" in df.columns:
    dim_date = df[["sale_date"]].drop_duplicates().rename(columns={"sale_date": "date"}).reset_index(drop=True)
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["quarter"] = dim_date["date"].dt.quarter
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["day_of_week"] = dim_date["date"].dt.day_name()
else:
    dim_date = pd.DataFrame(columns=["date", "year", "quarter", "month", "day", "day_of_week"])

fact_needed = ["sale_id", "sale_date", "store_id", "product_id", "quantity", "price", "revenue", "claim_id"]
warn_missing(df, fact_needed, "fact_sales")

fact_cols_available = [c for c in fact_needed if c in df.columns]
fact_sales = df[fact_cols_available].copy()

if "sale_date" in fact_sales.columns:
    fact_sales["sale_date"] = pd.to_datetime(fact_sales["sale_date"], errors="coerce")

for num_col in ["quantity", "price", "revenue"]:
    if num_col in fact_sales.columns:
        fact_sales[num_col] = pd.to_numeric(fact_sales[num_col], errors="coerce")

if "sale_id" in fact_sales.columns and fact_sales["sale_id"].isna().any():
    print("fact_sales has null values.")
if "revenue" in fact_sales.columns and (fact_sales["revenue"] < 0).any():
    print("fact_sales has negative values.")

paths = {
    "dim_store.parquet": dim_store,
    "dim_product.parquet": dim_product,
    "dim_category.parquet": dim_category,
    "dim_warranty.parquet": dim_warranty,
    "dim_date.parquet": dim_date,
    "fact_sales.parquet": fact_sales,
}

for fname, table in paths.items():
    outp = os.path.join(WH_DIR, fname)
    table.to_parquet(outp, index=False)
    print(f"Saved -> {outp}  ({table.shape[0]} rows, {table.shape[1]} columns)")

print("\nStar schema created.")



