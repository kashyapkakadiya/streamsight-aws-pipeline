import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
import warnings
warnings.filterwarnings("ignore")

# ──────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────
ENDPOINT_URL      = "https://localhost:4566"
AWS_REGION        = "us-east-1"
RAW_BUCKET        = "streamsight-raw"
PROCESSED_BUCKET  = "streamsight-processed"
RAW_KEY           = "spotify/Most Streamed Spotify Songs 2024.csv"
PROCESSED_PREFIX  = "spotify/processed/"

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=AWS_REGION,
    verify=False
)


# ──────────────────────────────────────────
# 1. EXTRACT — read CSV from S3
# ──────────────────────────────────────────
def extract():
    print("[EXTRACT] Reading CSV from S3...")
    response = s3.get_object(Bucket=RAW_BUCKET, Key=RAW_KEY)
    raw_bytes = response["Body"].read()
    try:
        content = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        content = raw_bytes.decode("latin-1")
    df = pd.read_csv(io.StringIO(content))
    print(f"[EXTRACT] Rows: {len(df)} | Columns: {len(df.columns)}")
    return df


# ──────────────────────────────────────────
# 2. TRANSFORM — clean and enrich
# ──────────────────────────────────────────
def transform(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[TRANSFORM] Starting transformations...")

    # --- 2a. Normalize column names ---
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "", regex=True)
    )
    print("[TRANSFORM] Column names normalized")

    # --- 2b. Drop duplicates ---
    before = len(df)
    df = df.drop_duplicates()
    print(f"[TRANSFORM] Dropped {before - len(df)} duplicate rows")

    # --- 2c. Cast numeric columns ---
    numeric_cols = [
        "spotify_streams", "spotify_playlist_count", "spotify_playlist_reach",
        "youtube_views", "youtube_likes", "tiktok_posts", "tiktok_likes",
        "tiktok_views", "pandora_streams", "soundcloud_streams",
        "apple_music_playlist_count", "deezer_playlist_count",
        "amazon_playlist_count", "shazam_counts"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
    print("[TRANSFORM] Numeric columns cast to int64")

    # --- 2d. Parse release date ---
    if "release_date" in df.columns:
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
        df["release_year"]  = df["release_date"].dt.year.astype("Int64")
        df["release_month"] = df["release_date"].dt.month.astype("Int64")
        print(f"[TRANSFORM] release_date parsed | Nulls: {df['release_date'].isna().sum()}")

    # --- 2e. Drop rows missing critical fields ---
    before = len(df)
    df = df.dropna(subset=["track", "artist"])
    print(f"[TRANSFORM] Dropped {before - len(df)} rows missing track/artist")

    # --- 2f. Add derived metrics ---
    df["cross_platform_reach"] = (
        df.get("spotify_streams", 0) +
        df.get("youtube_views", 0) +
        df.get("tiktok_views", 0)
    )
    print("[TRANSFORM] Added derived column: cross_platform_reach")

    # --- 2g. Add partition column for S3 ---
    df["partition_year"] = df["release_year"].fillna(0).astype(int)
    print("[TRANSFORM] Added partition_year column")

    print(f"[TRANSFORM] Done. Final rows: {len(df)}")
    return df


# ──────────────────────────────────────────
# 3. LOAD — write Parquet to S3
# ──────────────────────────────────────────
def load(df: pd.DataFrame):
    print("\n[LOAD] Writing Parquet files to S3...")

    # Convert to PyArrow table
    table = pa.Table.from_pandas(df)

    # Write to in-memory buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    # Upload to S3
    output_key = f"{PROCESSED_PREFIX}spotify_songs.parquet"
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=output_key,
        Body=buffer.getvalue()
    )
    print(f"[LOAD] Written to s3://{PROCESSED_BUCKET}/{output_key}")

    # Verify
    response = s3.list_objects_v2(Bucket=PROCESSED_BUCKET)
    for obj in response.get("Contents", []):
        size_kb = round(obj["Size"] / 1024, 2)
        print(f"[LOAD] Verified: {obj['Key']} ({size_kb} KB)")


# ──────────────────────────────────────────
# 4. ANALYTICS — run SQL-like queries
# ──────────────────────────────────────────
def analytics(df: pd.DataFrame):
    print("\n[ANALYTICS] Running post-load analytics...")

    # Top 10 most streamed songs
    top_songs = (
        df[["track", "artist", "spotify_streams"]]
        .sort_values("spotify_streams", ascending=False)
        .head(10)
    )
    print("\n[ANALYTICS] Top 10 Most Streamed Songs:")
    print(top_songs.to_string(index=False))

    # Top 10 artists by total streams
    top_artists = (
        df.groupby("artist")
        .agg(
            total_streams=("spotify_streams", "sum"),
            total_tracks=("track", "count"),
            avg_streams=("spotify_streams", "mean")
        )
        .sort_values("total_streams", ascending=False)
        .head(10)
        .reset_index()
    )
    top_artists["avg_streams"] = top_artists["avg_streams"].round(0).astype(int)
    print("\n[ANALYTICS] Top 10 Artists by Total Streams:")
    print(top_artists.to_string(index=False))

    # Yearly trends
    yearly = (
        df[df["release_year"].notna()]
        .groupby("release_year")
        .agg(
            total_tracks=("track", "count"),
            unique_artists=("artist", "nunique"),
            total_streams=("spotify_streams", "sum"),
            avg_cross_platform=("cross_platform_reach", "mean")
        )
        .sort_values("release_year", ascending=False)
        .head(10)
        .reset_index()
    )
    yearly["avg_cross_platform"] = yearly["avg_cross_platform"].round(0).astype(int)
    print("\n[ANALYTICS] Yearly Trends (Top 10 recent years):")
    print(yearly.to_string(index=False))


# ──────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────
if __name__ == "__main__":
    import time
    start = time.time()

    print("=" * 55)
    print("   STREAMSIGHT AWS GLUE ETL JOB")
    print("=" * 55)

    df_raw   = extract()
    df_clean = transform(df_raw)
    load(df_clean)
    analytics(df_clean)

    elapsed = round(time.time() - start, 2)
    print("\n" + "=" * 55)
    print(f"   JOB COMPLETE in {elapsed}s")
    print(f"   Records processed : {len(df_clean)}")
    print(f"   Output format     : Parquet (Snappy compressed)")
    print(f"   Output location   : s3://{PROCESSED_BUCKET}/{PROCESSED_PREFIX}")
    print("=" * 55)