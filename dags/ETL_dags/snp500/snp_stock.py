import boto3
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import FinanceDataReader as fdr
from dotenv import dotenv_values
import logging
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from io import StringIO
from typing import List, Dict

CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ
S3_BUCKET = CONFIG["S3_BUCKET"]
AWS_ACCESS_KEY = CONFIG["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY = CONFIG["AWS_SECRET_ACCESS_KEY"]
S3_FILE = "snp_stock.csv"
LOCAL_FILE = "./data/snp_stock.csv"
snp_list = pd.read_csv("./data/snp_stock_list.csv")  # 저장된 기업리스트 불러옴
symbols = snp_list["Symbol"].tolist()


def extract():
    all_dataframes = []

    def fetch_data(symbol):
        records = []
        df = fdr.DataReader(symbol, 2023)
        for index, row in df.iterrows():
            date = index.strftime("%Y-%m-%d")
            records.append(
                [
                    date,
                    row["Open"],
                    row["High"],
                    row["Low"],
                    row["Close"],
                    row["Volume"],
                    symbol,
                ]
            )

        return pd.DataFrame(
            records,
            columns=["Date", "Open", "High", "Low", "Close", "Volume", "Symbol"],
        )

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(fetch_data, symbol) for symbol in symbols]
        for future in futures:
            all_dataframes.append(future.result())

    raw_df = pd.concat(all_dataframes, ignore_index=True)
    raw_df.to_csv(f"./tmp/snp_stock.csv", index=False)

    return True


def transform():
    df = pd.read_csv(f"./tmp/snp_stock.csv")

    def remove_invalid_rows(df, column):
        df[column] = pd.to_numeric(df[column], errors="coerce")
        return df.dropna(subset=[column])

    # Remove rows with values ​​that cannot be converted to numbers for each column
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df = remove_invalid_rows(df, col)

    df = df.drop_duplicates()

    # Calculate the change
    df["Change"] = df["Close"].pct_change()
    # Replace NaN with 0
    df["Change"].fillna(0, inplace=True)

    df.to_csv(f"./data/snp_stock.csv", index=False)

    return True


def load():
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
    s3 = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
    )

    response = s3.get_object(Bucket=S3_BUCKET, Key=S3_FILE)
    body = response["Body"].read().decode("utf-8")

    # Convert to StringIO object (acts like a file object in memory)
    f = StringIO(body)
    next(f)  # skip header line

    DATABASE_USERNAME = CONFIG["POSTGRES_USER"]
    DATABASE_HOSTNAME = CONFIG["POSTGRES_HOST"]
    DATABASE_PORT = CONFIG["POSTGRES_PORT"]
    DATABASE_NAME = CONFIG["POSTGRES_DB"]
    DATABASE_PASSWORD = CONFIG["POSTGRES_PASSWORD"]

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
