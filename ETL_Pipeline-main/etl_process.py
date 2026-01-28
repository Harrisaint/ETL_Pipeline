"""
MedTech ETL Pipeline
Extracts messy biomedical data from S3, cleans it, and loads into PostgreSQL (RDS).

Pip install commands (run these first):
    pip install python-dotenv
    pip install boto3
    pip install pandas
    pip install sqlalchemy
    pip install psycopg2-binary
"""

import os
from typing import Optional

import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def extract_from_s3(bucket: str, key: str, local_path: str) -> pd.DataFrame:
    """Download S3_KEY from S3_BUCKET_NAME and save to LOCAL_DATA_PATH. Return DataFrame."""
    try:
        # S3 bucket is in us-east-2 (Ohio), use S3_REGION explicitly
        s3 = boto3.client("s3", region_name=os.getenv("S3_REGION", "us-east-2"))
        s3.download_file(bucket, key, local_path)
        print("S3 Download Complete")
    except Exception as e:
        err = str(e).lower()
        if "timeout" in err or "access denied" in err or "accessdenied" in err:
            raise ConnectionError(f"AWS S3 connection error (Timeout or Access Denied): {e}") from e
        raise

    df = pd.read_csv(local_path)
    print("Extract Complete")
    return df


def _col_heart_rate(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        if "heart" in c.lower() and "rate" in c.lower():
            return c
    return None


def _col_oxygen_sat(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        lower = c.lower()
        if ("oxygen" in lower and "sat" in lower) or "spo2" in lower or "oxygen saturation" in lower:
            return c
    return None


def _col_timestamp(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        if "timestamp" in c.lower():
            return c
    return None


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean biomedical data:
    - Remove duplicate rows
    - Fill missing Heart Rate and Oxygen Sat (SpO2) with median
    - Remove sensor-error outliers (Heart Rate > 200 or < 30)
    - Ensure Timestamp is proper datetime
    """
    # 1. Duplicates
    n_before = len(df)
    df = df.drop_duplicates()
    print(f"Transform: Removed {n_before - len(df)} duplicate rows")

    hr_col = _col_heart_rate(df)
    ox_col = _col_oxygen_sat(df)
    ts_col = _col_timestamp(df)

    # 2. Median imputation
    if hr_col:
        med = df[hr_col].median()
        df[hr_col] = df[hr_col].fillna(med)
        print(f"Transform: Filled missing '{hr_col}' with median {med}")
    if ox_col:
        med = df[ox_col].median()
        df[ox_col] = df[ox_col].fillna(med)
        print(f"Transform: Filled missing '{ox_col}' with median {med}")

    # 3. Outliers (Heart Rate)
    if hr_col:
        before = len(df)
        df = df[(df[hr_col] >= 30) & (df[hr_col] <= 200)]
        print(f"Transform: Removed {before - len(df)} Heart Rate outliers (>200 or <30)")

    # 4. Timestamp as datetime
    if ts_col:
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
        invalid = df[ts_col].isna().sum()
        if invalid:
            df = df.dropna(subset=[ts_col])
            print(f"Transform: Dropped {invalid} rows with invalid Timestamp")
        print("Transform: Timestamp converted to datetime")

    print("Data Cleaned")
    return df


def load_to_rds(df: pd.DataFrame, table: str = "clinical_vitals") -> None:
    """
    Connect to RDS PostgreSQL using environment variables and upload data.
    Connection string format: postgresql://{user}:{password}@{host}:{port}/{dbname}
    """
    host = os.getenv("RDS_HOSTNAME")
    port = os.getenv("RDS_PORT", "5432")
    dbname = os.getenv("RDS_DB_NAME", "postgres")
    user = os.getenv("RDS_USERNAME")
    password = os.getenv("RDS_PASSWORD")

    if not host:
        raise ValueError("RDS_HOSTNAME is not set in .env")
    if not user:
        raise ValueError("RDS_USERNAME is not set in .env")
    if not password:
        raise ValueError("RDS_PASSWORD is not set in .env")

    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Connection Successful")
    except Exception as e:
        print(f"Database connection error: {e}")
        raise

    df.to_sql(table, engine, if_exists="replace", index=False, method="multi", chunksize=1000)
    print("Database Upload Successful")


def main() -> None:
    bucket = os.getenv("S3_BUCKET_NAME")
    s3_key = os.getenv("S3_KEY", "messy_health_data.csv")
    local_path = os.getenv("LOCAL_DATA_PATH", "messy_health_data.csv")

    if not bucket:
        raise ValueError("S3_BUCKET_NAME is not set in .env")

    try:
        df = extract_from_s3(bucket, s3_key, local_path)
    except ConnectionError as e:
        print(f"AWS connection error: {e}")
        raise

    df_clean = transform_data(df)
    load_to_rds(df_clean)
    print("ETL Pipeline Complete")


if __name__ == "__main__":
    main()
