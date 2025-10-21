import os
from azure.storage.filedatalake import DataLakeServiceClient

connection_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
service_client = DataLakeServiceClient.from_connection_string(connection_str)
file_system_name = "apple-retail-data"

try:
    file_system_client = service_client.create_file_system(file_system_name)
    print(f"Container created: {file_system_name}")
except Exception:
    file_system_client = service_client.get_file_system_client(file_system_name)
    print(f"Container already exists: {file_system_name}")

folders = ["raw/apple", "processed/apple", "analytics/apple"]

for folder in folders:
    try:
        file_system_client.create_directory(folder)
        print(f"Folder created: {folder}")
    except Exception:
        print(f"Folder already exists: {folder}")
