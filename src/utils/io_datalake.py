import os
import io
import pandas as pd
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

def get_file_system_client():
    connection_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_str:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING in .env file.")
    service_client = DataLakeServiceClient.from_connection_string(connection_str)
    return service_client.get_file_system_client(file_system="apple-retail-data")

def read_from_datalake(remote_path: str) -> pd.DataFrame:
    fs_client = get_file_system_client()
    file_client = fs_client.get_file_client(remote_path)
    download = file_client.download_file()
    file_data = download.readall()
    df = pd.read_parquet(io.BytesIO(file_data))
    print(f"Read {remote_path} -> {df.shape[0]} rows, {df.shape[1]} cols")
    return df

def write_to_datalake(local_path: str, remote_path: str):
    fs_client = get_file_system_client()
    file_client = fs_client.create_file(remote_path)
    print(f"Uploading {local_path} to {remote_path}...")
    with open(local_path, "rb") as f:
        data = f.read()
        file_client.append_data(data, offset=0, length=len(data))
        file_client.flush_data(len(data))
    print("✅ Upload completed successfully.")

def list_files(remote_dir: str, include_dirs: bool = False) -> list[str]:
    fs_client = get_file_system_client()
    remote_dir = remote_dir.strip("/")
    results: list[str] = []
    for entry in fs_client.get_paths(path=remote_dir or None):
        if entry.is_directory:
            if include_dirs:
                results.append(entry.name)
        else:
            results.append(entry.name)
    print(f"Found {len(results)} objects under '{remote_dir or '/'}'")
    return results

def upload_large_file(file_client, local_path, chunk_size=4 * 1024 * 1024):
    """Upload large files in chunks (default: 4MB)."""
    file_size = os.path.getsize(local_path)
    print(f"Uploading {local_path} ({round(file_size/1024/1024,2)} MB)...")
    offset = 0
    with open(local_path, "rb") as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            file_client.append_data(chunk, offset=offset, length=len(chunk))
            offset += len(chunk)
    file_client.flush_data(offset)
    print(f"Completed upload: {local_path}")

def upload_to_azure(local_path, remote_path):
    fs_client = get_file_system_client()
    file_client = fs_client.create_file(remote_path)
    upload_large_file(file_client, local_path)

def upload_raw_files():
    local_dir = os.path.join(os.getcwd(), "data/raw/apple")
    remote_dir = "raw/apple"
    for file in os.listdir(local_dir):
        if file.endswith(".csv"):
            local_path = os.path.join(local_dir, file)
            remote_path = f"{remote_dir}/{file}"
            upload_to_azure(local_path, remote_path)

if __name__ == "__main__":
    upload_raw_files()
