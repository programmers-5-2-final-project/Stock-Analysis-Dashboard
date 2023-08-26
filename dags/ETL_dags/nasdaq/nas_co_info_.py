
# nas_com_info.py
import pandas as pd
import os
import boto3
import logging
import psycopg2
import FinanceDataReader as fdr
import yfinance as yf
from io import StringIO
from dotenv import dotenv_values

from concurrent.futures import ThreadPoolExecutor
import sys
import os


sys.path.append(
    os.path.dirname(
        os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    )
)


from ETL_dags.common.extract import Extract
from ETL_dags.common.csv import df_to_csv, csv_to_df
from ETL_dags.nasdaq.constants import FilePath



CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ


class FetchDataError(Exception):
    """Custom error for fetch_data function."""

    pass


def extract():
    header = [
        "symbol",
        "shortName",
        "sector",
        "marketCap",
        "volume",
        "previousClose",
        "regularMarketOpen",
        "change",
    ]
    def getinfo(symbol):
    # nas_Symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
    df_nas = fdr.StockListing("NASDAQ")
    nas_Symbols = list(set(df_nas["Symbol"].tolist()))

    company_info_list = []
    company_info_list.append(header)

    # 각 기업의 정보를 DataFrame에 추가
    for symbol in nas_Symbols:
        try:
            nasdaq_tickers = yf.Tickers(symbol)
            ticker_data = nasdaq_tickers.tickers[symbol].info
            info = [ticker_data[item] for item in header[:-1]]
            info.append(round(((info[-1] - info[-2]) / info[-2]) * 100, 4))
            print(f"Successfully fetched data for symbol {symbol}")
            return info
        except Exception as e:
            print(f"Failed to fetch data for symbol {symbol}. Error: {str(e)}")
            return [symbol] + [None] * (len(header) - 1)

    df_nas = fdr.StockListing("NASDAQ")
    nas_Symbols = list(set(df_nas["Symbol"].tolist()))
    company_info_list = []
    company_info_list.append(header)

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(getinfo, symbol) for symbol in nas_Symbols]
        for future in futures:
            company_info_list.append(future.result())

        # header에 해당하는 정보로 데이터프레임 생성
    df = pd.DataFrame(company_info_list[1:], columns=header)
    nas_co_info_filepath = "./data/nas_co_info.csv"
    df_sorted = df.sort_values(by="marketCap", ascending=False)
    df_sorted = df_sorted.dropna(subset=["shortName"])
    df_sorted.to_csv(nas_co_info_filepath, mode="w", index=False, header=True)

    print(f"Company info saved to {nas_co_info_filepath}")


def load():
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ
    nas_com_info_filepath = "./data/nas_co_info.csv"
    s3 = boto3.resource(  # s3 연결 객체
        "s3",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"],
    )
    with open(nas_com_info_filepath, "rb") as f:
        s3.Bucket("de-5-2").put_object(Key=f"nas_co_info.csv", Body=f)


def rds():
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ
    s3 = boto3.client(  # s3 연결 객체
        "s3",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"],
    )
    # S3 버킷과 파일 이름 설정
    bucket_name = "de-5-2"
    file_name = "nas_co_info.csv"

    # S3에서 파일 내용 가져오기
    s3_response = s3.get_object(Bucket=bucket_name, Key=file_name)
    csv_content = s3_response["Body"].read().decode("utf-8")

    # StringIO 객체로 변환 (메모리 상에서의 파일 객체처럼 동작)
    f = StringIO(csv_content)
    next(f)  # 헤더 행 건너뛰기

    # PostgreSQL RDS 연결 정보 설정
    db_host = CONFIG["POSTGRES_HOST"]
    db_port = CONFIG["POSTGRES_PORT"]
    db_name = "dev"
    db_user = CONFIG["POSTGRES_USER"]
    db_password = CONFIG["POSTGRES_PASSWORD"]

    # RDS 연결
    connection = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )
    cursor = connection.cursor()

    try:
        # 새로운 데이터 테이블 생성
        table_name = "nas_co_info"
        create_table_query = f"""
            DROP TABLE IF EXISTS raw_data.{table_name};
            CREATE TABLE raw_data.{table_name} (
                symbol VARCHAR(100),
                name VARCHAR(100),
                sector VARCHAR(100),
                marcap FLOAT,
                volume FLOAT,
                previous_close FLOAT,
                regular_market_open FLOAT,
                changesratio FLOAT
            );"""

        cursor.execute(create_table_query)

        # copy_expert를 사용하여 메모리에서 바로 Postgres로 데이터 업로드
        copy_sql = """
        COPY raw_data.nas_co_info FROM stdin WITH CSV DELIMITER ','
        """
        cursor.copy_expert(sql=copy_sql, file=f)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    finally:
        # 데이터베이스 연결 종료
        cursor.close()
        connection.close()


# extract()
# load()
# rds()

