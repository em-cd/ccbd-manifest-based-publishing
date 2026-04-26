import subprocess
from upload import upload

def run(cmd):
    subprocess.run(cmd, check=True)

def main():
    print("1. Generating datasets...")
    run(["python", "dataset_gen.py"])

    print("2. Uploading datasets...")   
    upload("data/test/test_output.parquet", "raw/test/test_output.parquet")

    print("Done.")

if __name__ == "__main__":
    main()