import boto3
import os

# ──────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────
ENDPOINT_URL = "https://localhost:4566"
AWS_REGION = "us-east-1"
RAW_BUCKET = "streamsight-raw"
PROCESSED_BUCKET = "streamsight-processed"
CSV_FILE = "../data/Most Streamed Spotify Songs 2024.csv"

# boto3 client pointing to LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=AWS_REGION,
    verify=False  # skip SSL verification for LocalStack
)


def create_buckets():
    print("[SETUP] Creating S3 buckets...")
    for bucket in [RAW_BUCKET, PROCESSED_BUCKET]:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"[SETUP] Created bucket: {bucket}")
        except s3.exceptions.BucketAlreadyOwnedByYou:
            print(f"[SETUP] Bucket already exists: {bucket}")


def upload_raw_data():
    print("\n[SETUP] Uploading raw CSV to S3...")
    filepath = os.path.abspath(CSV_FILE)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV not found at: {filepath}")

    s3.upload_file(
        Filename=filepath,
        Bucket=RAW_BUCKET,
        Key="spotify/Most Streamed Spotify Songs 2024.csv"
    )
    print(f"[SETUP] Uploaded to s3://{RAW_BUCKET}/spotify/")


def verify_upload():
    print("\n[SETUP] Verifying upload...")
    response = s3.list_objects_v2(Bucket=RAW_BUCKET)
    for obj in response.get("Contents", []):
        size_kb = round(obj["Size"] / 1024, 2)
        print(f"[SETUP] Found: {obj['Key']} ({size_kb} KB)")


if __name__ == "__main__":
    print("=" * 50)
    print("   STREAMSIGHT AWS INFRASTRUCTURE SETUP")
    print("=" * 50)
    create_buckets()
    upload_raw_data()
    verify_upload()
    print("\n[SETUP] Infrastructure ready.")
    print(f"  Raw bucket      : s3://{RAW_BUCKET}/")
    print(f"  Processed bucket: s3://{PROCESSED_BUCKET}/")
    print("=" * 50)