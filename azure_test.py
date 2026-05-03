# test_azure.py
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

account = os.getenv("AZURE_STORAGE_ACCOUNT")
key = os.getenv("AZURE_STORAGE_KEY")
container = os.getenv("AZURE_CONTAINER")

blob_service = BlobServiceClient(
    account_url=f"https://{account}.blob.core.windows.net",
    credential=key
)

container_client = blob_service.get_container_client(container)

# List blobs (should be empty)
print(f"🪣 Container: {container}\n")
blobs = list(container_client.list_blobs())
if blobs:
    for b in blobs:
        print(f"  📄 {b.name} ({b.size / 1e6:.1f} MB)")
else:
    print("  📭 Empty — ready to go!")