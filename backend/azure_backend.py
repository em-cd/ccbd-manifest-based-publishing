import os
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
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
      max_block_size=4 * 1024 * 1024,        # 4 MB chunks (default is 4MB)
      max_single_put_size=8 * 1024 * 1024,    # Files under 8MB upload in one shot
      max_page_size=4 * 1024 * 1024,
      connection_timeout=300,
      read_timeout=300,
    )

    self.container_client = blob_service.get_container_client(self.container)

  def list_objects(self, prefix):
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
    if not key:
      key = os.path.basename(local_path)

    # Ensure local directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    blob_client = self.container_client.get_blob_client(key)

    try:
      with open(local_path, "wb") as f:
        stream = blob_client.download_blob()
        f.write(stream.readall())
    except AzureError as e:
      raise RuntimeError(f"Download failed: {e}")

    return os.path.getsize(local_path)
 
  def upload_file(self, file_path: str, key: str = None):
    if not os.path.exists(file_path):
      raise FileNotFoundError(f"{file_path} does not exist")

    if not key:
      key = os.path.basename(file_path)

    blob_client = self.container_client.get_blob_client(key)

    try:
      with open(file_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    except AzureError as e:
      raise RuntimeError(f"Upload failed: {e}")
   
    return os.path.getsize(file_path)

  def filesystem(self):
    from adlfs import AzureBlobFileSystem
    return AzureBlobFileSystem(
      account_name=self.account,
      account_key=self.key
    )
  
  def get_root(self):
    return self.container