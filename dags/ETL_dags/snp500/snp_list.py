# S&P 500 전체
import FinanceDataReader as fdr
import pandas as pd
import boto3
import psycopg2
from io import StringIO
from dotenv import dotenv_values
import os

"""
제공되는 값.
Symbol,Name,Sector,Industry
MMM,3M,Industrials,Industrial Conglomerates
"""
CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ
AWS_ACCESS_KEY = CONFIG["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY = CONFIG["AWS_SECRET_ACCESS_KEY"]

DATABASE_USERNAME = CONFIG["RDS_USER"]
DATABASE_HOSTNAME = CONFIG["RDS_HOST"]
DATABASE_PORT = CONFIG["RDS_PORT"]
DATABASE_NAME = CONFIG["RDS_DB"]
DATABASE_PASSWORD = CONFIG["RDS_PASSWORD"]
S3_BUCKET = CONFIG["S3_BUCKET"]


def extract():
    # filepath = "/data/krx_list.csv"
    fdr_symbol = fdr.StockListing("S&P500")
    fdr_symbol.to_csv(
        "./tmp/snp_stock_list.csv", index=False, encoding="utf-8-sig"
    )  # 다음 테스크로 데이터를 이동시키기 위해 csv 파일로 저장
    return True


def transform():
    raw_df = pd.read_csv(f"./tmp/snp_stock_list.csv")  # 저장된 데이터를 다시 DataFrame으로 로드
    transformed_df = raw_df.dropna(how="all")

    transformed_df.to_csv(
        f"./data/snp_stock_list.csv", index=False
    )  # 다음 태스크로 데이터를 이동하기 위해 csv 파일로 저장
    return True


def load():
    S3_FILE = "snp_stock_list.csv"
    LOCAL_FILE = "./data/snp_stock_list.csv"
    s3 = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
    )

    try:
        with open(LOCAL_FILE, "rb") as file:
            s3.upload_fileobj(file, S3_BUCKET, S3_FILE)
        return True
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return False


def s3_to_rds():
    S3_FILE = "snp_stock_list.csv"

    s3 = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
    )

    response = s3.get_object(Bucket=S3_BUCKET, Key=S3_FILE)
    body = response["Body"].read().decode("utf-8")

    # Convert to StringIO object (acts like a file object in memory)
    f = StringIO(body)
    next(f)  # skip header line

    conn = psycopg2.connect(
        dbname=DATABASE_NAME,
        user=DATABASE_USERNAME,
        password=DATABASE_PASSWORD,
        host=DATABASE_HOSTNAME,
        port=DATABASE_PORT,
    )
    cur = conn.cursor()

    try:
        cur.execute(
            """
            DROP TABLE IF EXISTS raw_data.snp_stock;
            CREATE TABLE raw_data.snp_stock(
                date VARCHAR(40),
                Open VARCHAR(40),
                High VARCHAR(40),
                Low VARCHAR(40),
                Close VARCHAR(40),
                Volume VARCHAR(40),
                Symbol VARCHAR(40),
                Change VARCHAR(40)
            );
        """
        )

        # Upload data to Postgres directly from memory using copy_expert
        copy_sql = """
            COPY raw_data.snp_stock FROM stdin WITH CSV DELIMITER ','
        """
        cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()

        cur.execute(
            """
            ALTER TABLE raw_data.snp_stock
                ALTER COLUMN date TYPE DATE USING date::DATE,
                ALTER COLUMN Open TYPE FLOAT USING Open::FLOAT,
                ALTER COLUMN High TYPE FLOAT USING High::FLOAT,
                ALTER COLUMN Low TYPE FLOAT USING Low::FLOAT,
                ALTER COLUMN Close TYPE FLOAT USING Close::FLOAT,
                ALTER COLUMN Volume TYPE FLOAT USING Volume::FLOAT,
                ALTER COLUMN Change TYPE FLOAT USING Change::FLOAT
            ;
        """
        )
        conn.commit()

    except Exception as e:
        print(f"Error occurred: {e}")
        return False

    finally:
        cur.close()
        conn.close()

    return True
