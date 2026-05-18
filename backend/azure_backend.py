import os
import json
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError, ResourceNotFoundError
from dotenv import load_dotenv

# Load .env into environment variables
load_dotenv()

class AzureBackend:
  def __init__(self):
    self.account = os.getenv("AZURE_STORAGE_ACCOUNT")
    self.key = os.getenv("AZURE_STORAGE_KEY")
    self.container = os.getenv("AZURE_CONTAINER")

    if not self.account or not self.key:
      raise ValueError("Azure credentials missing in .env")
  
    if not self.container:
      raise ValueError("Azure container missing in .env")

    blob_service = BlobServiceClient(
        account_url=f"https://{self.account}.blob.core.windows.net",
        credential=self.key,
        max_block_size=8 * 1024 * 1024,       # 8 MB to match S3 multipart_chunksize
        max_single_put_size=8 * 1024 * 1024,  # 8 MB to match S3 multipart_threshold
        connection_timeout=600,
        read_timeout=600,
    )

    self.container_client = blob_service.get_container_client(self.container)

  def filesystem(self):
    from adlfs import AzureBlobFileSystem
    return AzureBlobFileSystem(
      account_name=self.account,
      account_key=self.key
    )

  def get_root(self):
    return self.container

  def list_objects(self, prefix):
    """
    List objects and page count in blob storage
    """
    keys = []
    page_count = 0

    pages = self.container_client.list_blobs(
        name_starts_with=prefix
    ).by_page() 

    for page in pages:
        page_count += 1
        for blob in page:
            keys.append(blob.name)

    return keys, page_count

  def download_file(self, local_path: str, key: str = None):
    """
    Download a single file from blob storage
    """
    if not key:
        key = os.path.basename(local_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    blob_client = self.container_client.get_blob_client(key)
    try:
        with open(local_path, "wb") as f:
            stream = blob_client.download_blob(max_concurrency=10)
            f.write(stream.readall())  
    except AzureError as e:
        raise RuntimeError(f"Download failed: {e}")
    return os.path.getsize(local_path)

  def upload_file(self, file_path: str, key: str = None):
    """
    Upload a single file to blob storage
    """
    if not os.path.exists(file_path):
      raise FileNotFoundError(f"{file_path} does not exist")

    if not key:
      key = os.path.basename(file_path)

    blob_client = self.container_client.get_blob_client(key)

    try:
      with open(file_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True, max_concurrency=10) #match 10 concurrent threads in aws
    except AzureError as e:
      raise RuntimeError(f"Upload failed: {e}")
   
    return os.path.getsize(file_path)

  def write_json(self, key: str, data: dict):
    """
    Write a Python dict as JSON to Azure Blob Storage.
    """
    blob_client = self.container_client.get_blob_client(key)

    try:
        payload = json.dumps(data, indent=2).encode("utf-8")
        blob_client.upload_blob(payload, overwrite=True)
    except AzureError as e:
        raise RuntimeError(f"write_json failed: {e}")

  def read_json(self, key: str):
      """
      Read JSON blob and return as dict. Returns None if not found.
      """
      blob_client = self.container_client.get_blob_client(key)

      try:
          stream = blob_client.download_blob()
          data = stream.readall()
          return json.loads(data.decode("utf-8"))
      except ResourceNotFoundError:
          return None
      except AzureError as e:
          raise RuntimeError(f"read_json failed: {e}")

  def delete_prefix(self, prefix: str):
      """
      Delete all blobs under a prefix.
      """
      keys, _ = self.list_objects(prefix)

      if not keys:
          print(f"No objects found under {prefix}")
          return 0

      deleted = 0

      # Azure allows batch deletion (up to ~256–1000 depending on SDK version)
      batch_size = 256

      for i in range(0, len(keys), batch_size):
          batch = keys[i:i + batch_size]

          try:
              self.container_client.delete_blobs(*batch)
              deleted += len(batch)
          except AzureError as e:
              raise RuntimeError(f"delete_prefix failed: {e}")

      print(f"Deleted {deleted} objects under {prefix}")
      return deleted