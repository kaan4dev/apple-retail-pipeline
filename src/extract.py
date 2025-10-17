import os 
import pandas as pd

CURRENT_DIR = os.getcwd()
while not CURRENT_DIR.endswith("apple-retail-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)

os.chdir(CURRENT_DIR)
print(f"Working directory is set to: ", CURRENT_DIR)

RAW_DIR = os.path.join(CURRENT_DIR, "data/raw/apple")
OUT_DIR = os.path.join(OUT_DIR, "data/processed/apple")
os.makedirs(OUT_DIR, exist_ok = True)

files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]  
if not files :
    raise FileNotFoundError(f"No csv files found in {RAW_DIR}")

dataframes = {}
for file in files:
    path = os.path.join(RAW_DIR, file)
    name = file.replace(".csv", "")
    df = pd.read_csv(path)
    dataframes[name] = df
    print(f"Loaded {file} -> {df.shape[0]} rows, {df.shape[1]} cols")

for name, df in dataframes.items():
    print(f"\n Preview of {name}: ")
    print(df.head(3))

for name, df in dataframes.items():
    out_path = os.path.join(OUT_DIR, f"raw_{name}.parquet")
    df.to_parquet(out_path, index= False)
    print(f"Saved to {out_path}")
