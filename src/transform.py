import os 
import pandas as pd

CURRENT_DIR = os.getcwd()
while not CURRENT_DIR.endswith("apple-retail-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)
os.chdir(CURRENT_DIR)
print("Working directory set to:", os.getcwd())

RAW_DIR = os.path.join(CURRENT_DIR, "data/raw/apple")
PROCESSED_DIR = os.path.join(CURRENT_DIR, "data/processed/apple")
os.makedirs(PROCESSED_DIR, exist_ok=True)

sales = pd.read_csv(os.path.join(RAW_DIR, "sales.csv"))
stores = pd.read_csv(os.path.join(RAW_DIR, "stores.csv"))
products = pd.read_csv(os.path.join(RAW_DIR, "products.csv"))
category = pd.read_csv(os.path.join(RAW_DIR, "category.csv"))
warranty = pd.read_csv(os.path.join(RAW_DIR, "warranty.csv"))

print("Data loaded.")
print(f"Sales Shape: {sales.shape}")

# step 1 -> normalize column names 
for df in [sales, stores, products, category, warranty]:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

# step 2 -> clean null values and format dates
sales.dropna(how="all", inplace=True)
if "sale_date" in sales.columns:
    sales["sale_date"] = pd.to_datetime(sales["sale_date"], errors="coerce", dayfirst=True)

# step 3 -> join the fact tables
products_merged = products.merge(category, how="left", on="category_id", suffixes=("", "_cat"))
sales_merged = sales.merge(stores, how="left", on="store_id")
final_df = sales_merged.merge(products_merged, how="left", on="product_id")

if "sale_id" in warranty.columns:
    final_df = final_df.merge(warranty, how="left", on="sale_id", suffixes=("", "_warranty"))
else:
    print("'sale_id' not found in warranty table — skipping warranty merge")

# step 4 -> feature engineering: calculate revenue if price exists
if {"quantity", "price"}.issubset(final_df.columns):
    final_df["revenue"] = final_df["quantity"] * final_df["price"]
else:
    print("'quantity' or 'price' column not found, check products.csv")

# step 5 -> reorder column by names
ordered_cols = [
    "sale_id", "sale_date", "store_id", "store_name", "region",
    "product_id", "product_name", "category_id", "category_name",
    "quantity", "price", "revenue",
    "claim_id", "claim_date", "repair_status"
]
final_df = final_df[[c for c in ordered_cols if c in final_df.columns]]

print(f"Final dataframe shape: {final_df.shape}")
print(final_df.head(5))


output_path = os.path.join(PROCESSED_DIR, "processed_sales.parquet")
final_df.to_parquet(output_path, index=False)
print(f"Saved transformed data to: {output_path}")
