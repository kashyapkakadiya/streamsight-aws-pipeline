```markdown
# StreamSight AWS Glue ETL Pipeline

An AWS Glue-style ETL pipeline that reads raw data from S3, transforms
it using pandas, and writes Snappy-compressed Parquet files back to S3
— fully simulated locally using LocalStack.

## Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Cloud Simulation | LocalStack 3.0 |
| Object Storage | Amazon S3 (LocalStack) |
| ETL Script | AWS Glue-style (boto3 + pandas) |
| Output Format | Parquet (Snappy compressed) |
| AWS SDK | boto3 |

## Architecture

```
s3://streamsight-raw/spotify/*.csv

           ↓
           
   glue_job.py (ETL)
   
   - Extract: s3.get_object → pandas DataFrame
     
   - Transform: normalize → deduplicate → cast → parse dates → enrich
     
   - Load: PyArrow → Parquet → s3.put_object
     
           ↓
     
s3://streamsight-processed/spotify/processed/*.parquet
```

## Project Structure

```
streamsight-aws-pipeline/

├── docker-compose.yml        # LocalStack container

├── infra/

│   └── setup.py              # creates S3 buckets, uploads raw CSV

├── scripts/

│   └── glue_job.py           # Glue-style ETL job

├── data/                     # CSV dataset (git-ignored)

└── .gitignore
```

## ETL Stages

### Extract
Reads raw CSV directly from S3 using `s3.get_object()`.
Handles mixed encodings (UTF-8 + Latin-1 fallback).

### Transform
Applies 7 transformation steps:
- Normalizes all column names to snake_case
- Drops 2 fully duplicate rows
- Strips commas and casts 14 numeric columns to int64
- Parses release dates and extracts release_year and release_month
- Drops 5 rows missing track name or artist name
- Adds derived column: cross_platform_reach (Spotify + YouTube + TikTok)
- Adds partition_year column for S3 partitioning pattern

Final output: **4,593 clean records**

### Load
Converts DataFrame to PyArrow table and writes Snappy-compressed
Parquet to `s3://streamsight-processed/`. Output file: 799.98 KB
vs 1,072 KB raw CSV — 25% size reduction from Parquet compression alone.

## How to Run

### Prerequisites
- Docker Desktop installed and running
- Python 3.11+
- Dependencies: `pip install boto3 pandas pyarrow awscli-local`

### Steps

```bash
# 1. Clone the repo
git clone https://github.com/kashyapkakadiya/streamsight-aws-pipeline.git
cd streamsight-aws-pipeline

# 2. Start LocalStack
docker-compose up -d

# 3. Wait ~10 seconds, then verify LocalStack is ready
curl -k https://localhost:4566/_localstack/health

# 4. Add your dataset
# Download from Kaggle and place at:
# data/Most Streamed Spotify Songs 2024.csv

# 5. Create S3 buckets and upload raw data
cd infra
python setup.py

# 6. Run the Glue ETL job
cd ../scripts
python glue_job.py
```

### Stop LocalStack
```bash
docker-compose down
```

## Key Concepts Demonstrated

- Reading and writing files directly to/from S3 using boto3
- Glue-style ETL script pattern (identical to production AWS Glue jobs)
- Parquet as output format with Snappy compression
- In-memory Parquet serialization using PyArrow (no temp files)
- LocalStack as a zero-cost AWS simulation environment
- S3 data lake structure with raw and processed bucket separation

## Dataset

Source: [Most Streamed Spotify Songs 2024](https://www.kaggle.com/datasets/nelgiriyewithana/most-streamed-spotify-songs-2024)
Records: 4,600 | Raw size: 1.05 MB | Processed size: 799.98 KB (Parquet)
```
