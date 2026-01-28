# ETL Pipeline - Medical Data Processing

Production-grade ETL pipeline for processing medical/vital signs data from S3 to AWS RDS PostgreSQL.

## Pip install commands (run these first)

```bash
pip install python-dotenv
pip install boto3
pip install pandas
pip install sqlalchemy
pip install psycopg2-binary
```

Or install from `requirements.txt`:

```bash
pip install -r requirements.txt
```

## Setup

1. **Install Dependencies** (see pip commands above)

2. **Configure Environment Variables**
   - Copy `.env.template` to `.env`
   - Fill in your AWS and RDS configuration:
     ```bash
     cp .env.template .env
     ```
   - Edit `.env` with your actual values:
     - `RDS_SECRET_NAME`: Your AWS Secrets Manager secret name
     - `S3_BUCKET_NAME`: Your S3 bucket name
     - `RDS_WRITER_ENDPOINT`: Your RDS cluster writer endpoint
     - `AWS_REGION`: Your AWS region

3. **AWS Credentials**
   - Configure AWS credentials via AWS CLI: `aws configure`
   - Or set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`

## Usage

Run the ETL pipeline:
```bash
python etl_process.py
```

## Pipeline Stages

1. **Extract**: Downloads `messy_health_data.csv` from S3
2. **Transform**: 
   - Removes duplicate rows
   - Fills missing Heart Rate and Oxygen Sat (SpO2) with median
   - Removes sensor-error outliers (Heart Rate > 200 or < 30)
   - Converts Timestamp column to proper datetime format
3. **Load**: Uploads cleaned data to RDS table `clinical_vitals`

## Requirements

- Python 3.8+
- AWS account with S3, Secrets Manager, and RDS access
- PostgreSQL database on AWS RDS