import boto3
import snowflake.connector
import os
import tempfile
from dotenv import load_dotenv
load_dotenv()
# --- MinIO config (to fetch the transformed file) ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
TRANSFORMED_BUCKET = "spotify-transformed-data"
TRANSFORMED_KEY = "transformed/spotify_tracks_cleaned.csv"
LOCAL_FILE = os.path.join(tempfile.gettempdir(), "spotify_tracks_cleaned.csv")

# --- Snowflake config (fill in your real values) ---
SNOWFLAKE_CONFIG = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "spotify_wh"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "spotify_db"),
    "schema": os.environ.get("SNOWFLAKE_SCHEMA", "pipeline"),
}


def download_from_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    s3.download_file(TRANSFORMED_BUCKET, TRANSFORMED_KEY, LOCAL_FILE)
    print(f"Downloaded {TRANSFORMED_KEY} from MinIO -> {LOCAL_FILE}")


def load_to_snowflake():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()

    try:
        # 1. Create an internal stage — Snowflake's own temporary storage area for file uploads
        cur.execute("CREATE STAGE IF NOT EXISTS spotify_stage")

        # 2. PUT: upload the local file into that stage (this is the actual file transfer)
        local_path = os.path.abspath(LOCAL_FILE).replace("\\", "/")
        cur.execute(f"PUT file://{local_path} @spotify_stage OVERWRITE = TRUE AUTO_COMPRESS = FALSE")
        print("File uploaded to Snowflake internal stage.")

        # 3. Clear the table first (safe to re-run this script without duplicating data)
        cur.execute("TRUNCATE TABLE spotify_tracks")

        # 4. COPY INTO: load the staged file into the actual table
        staged_filename = os.path.basename(LOCAL_FILE)
        copy_sql = f"""
            COPY INTO spotify_tracks
            FROM @spotify_stage/{staged_filename}
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_sql)
        result = cur.fetchall()
        print("COPY INTO result:")
        for row in result:
            print(row)

        # 5. Quick sanity check
        cur.execute("SELECT COUNT(*) FROM spotify_tracks")
        count = cur.fetchone()[0]
        print(f"\nTotal rows now in spotify_tracks: {count}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    download_from_minio()
    load_to_snowflake()