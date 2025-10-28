from __future__ import annotations
import logging
import os
import tempfile
import shutil
from typing import List, Dict
from azure.storage.blob import BlobServiceClient


def list_event_files(connection_string: str, container: str, environment: str) -> List[Dict[str, str]]:
    """
    List all event database files from storage
    
    Returns:
        List of dicts with 'path' and 'name' keys
    """
    files = []
    
    if environment == "local":
        # List files from local storage
        local_storage_dir = os.path.join(os.path.dirname(os.getcwd()),"salesforce_client","data")
        logging.info(f"local_storage_dir:{local_storage_dir}")
        
        if not os.path.exists(local_storage_dir):
            return files
        
        for filename in os.listdir(local_storage_dir):
            if filename.endswith(".db") and filename.startswith("events"):
                full_path = os.path.join(local_storage_dir, filename)
                files.append({
                    "path": full_path,
                    "name": filename
                })
    else:
        # List files from Azure Blob Storage
        service = BlobServiceClient.from_connection_string(connection_string)
        container_client = service.get_container_client(container)
        
        # List blobs with 'events/' prefix
        blobs = container_client.list_blobs(name_starts_with="events/")
        
        for blob in blobs:
            if blob.name.endswith(".db") and blob.name.startswith("events"):
                files.append({
                    "path": blob.name,
                    "name": os.path.basename(blob.name)
                })
    
    return files


def download_file(connection_string: str, container: str, blob_path: str, environment: str) -> str:
    """
    Download a file from storage to a temporary location
    
    Returns:
        Local path to the downloaded file
    """
    if environment == "local":
        # Copy from local storage
        source_path = blob_path  # full path from list_event_files
        temp_path = os.path.join(tempfile.gettempdir(), os.path.basename(blob_path))
        
        shutil.copy2(source_path, temp_path)
        return temp_path
    else:
        # Download from Azure Blob Storage
        service = BlobServiceClient.from_connection_string(connection_string)
        blob_client = service.get_blob_client(container=container, blob=blob_path)
        
        # Create temp file
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, os.path.basename(blob_path))
        
        with open(temp_path, "wb") as f:
            blob_data = blob_client.download_blob()
            blob_data.readinto(f)
        
        return temp_path

