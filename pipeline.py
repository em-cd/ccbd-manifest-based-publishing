import os
from dataset_gen import generate_dataset
from upload import upload

def main():
    print("1. Generating datasets...")

    result = generate_dataset("test")

    print("2. Uploading datasets...")
    for file_path in result["file_paths"]:
        s3_key = f"raw/{result['size_label']}/{os.path.basename(file_path)}"
        try:
            upload(file_path, s3_key)
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    print("Done.")

if __name__ == "__main__":
    main()