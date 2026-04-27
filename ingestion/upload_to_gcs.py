"""
M5 Dataset GCS Upload Script
Project: walmart-demand-intelligence
Author:  May Tilokruangchai
"""

from google.cloud import storage
import os
import glob

# ── Config ────────────────────────────────────────────────
PROJECT_ID  = "walmart-demand-intelligence"
BUCKET_NAME = "walmart-demand-raw-stilok19"
DATA_DIR    = "data/"
# ──────────────────────────────────────────────────────────

def upload_files_to_gcs(
    data_dir:    str = DATA_DIR,
    bucket_name: str = BUCKET_NAME,
    project_id:  str = PROJECT_ID
):
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))

    if not csv_files:
        print(f"⚠️  No CSV files found in {data_dir}")
        return

    print(f"Found {len(csv_files)} files to upload...")
    print(f"Destination: gs://{bucket_name}/raw/")
    print()

    uploaded = 0
    failed   = 0

    for filepath in csv_files:
        filename    = os.path.basename(filepath)
        destination = f"raw/{filename}"

        try:
            blob = bucket.blob(destination)
            blob.upload_from_filename(filepath)
            size = os.path.getsize(filepath) / (1024*1024)
            print(f"  ✅ {filename:<45} ({size:.1f} MB)")
            uploaded += 1
        except Exception as e:
            print(f"  ❌ {filename} — {e}")
            failed += 1

    print()
    print(f"Done! {uploaded} uploaded, {failed} failed")

if __name__ == "__main__":
    upload_files_to_gcs()
