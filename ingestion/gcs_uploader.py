from google.cloud import storage
import logging
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def upload_to_gcs(local_path, bucket_name, gcs_prefix, date = None):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for root, dirs, files in os.walk(local_path):
        if date and date not in root:
            continue
        for file in files:
            file_path = os.path.join(root, file)
            if not file.endswith(".crc"):
                relative_path = os.path.relpath(file_path, local_path)
                blob = bucket.blob(f"{gcs_prefix}/{relative_path}")
                blob.upload_from_filename(file_path)
                logger.info(f"Uploaded {file_path} to gs://{bucket_name}/{gcs_prefix}/{relative_path}")
                
                
if __name__ == "__main__":
    load_dotenv()
    upload_to_gcs(
        local_path=os.getenv("SILVER_PATH"),
        bucket_name=os.getenv("BUCKET_NAME"),
        gcs_prefix="silver"
    )