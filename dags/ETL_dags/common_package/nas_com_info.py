# nas_com_info.py
import requests
import pandas as pd
import os
import time
import boto3
import logging
import psycopg2
from io import StringIO
from dotenv import dotenv_values

task_logger = logging.getLogger("airflow.task")
task_logger.info("nas_com_info ", os.path.dirname)

CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ
api = CONFIG["ALPHA_VANTAGE"]


class FetchDataError(Exception):
    """Custom error for fetch_data function."""

    pass


def com_info(symbol):
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={api}"

    try:
        r = requests.get(url)
        r.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        data = r.json()
        if "Error Message" in data:
            # Check if the API returned an error message
            raise FetchDataError(data["Error Message"])
        return pd.DataFrame([data])

    except requests.RequestException as e:
        # This will catch any request-related exceptions (like connection errors)
        raise FetchDataError(
            f"Failed to fetch data for symbol {symbol}. Error: {str(e)}"
        )


def extract():
    symbols = [
        "AAPL",
        "MSFT",
        "GOOGL",
        "AMZN",
        "NVDA",
        "META",
        "TSLA,",
        "PEP",
        "AVGO",
        "ASML",
        "AZN",
        "COST",
        "CSCO",
        "CMCSA",
        "TMUS",
        "ADBE",
        "TXN",
        "NFLX",
        "SNY",
        "HON",
        "AMD",
        "INTC",
        "QCOM",
        "AMGN",
        "INTU",
        "ISRG",
        "MDLZ",
        "GILD",
        "BKNG",
        "AMAT",
        "ADI",
        "ADP",
        "VRTX",
        "REGN",
        "PDD",
        "PYPL",
        "ABNB",
        "FISV",
        "LRCX",
        "MU",
        "EQIX",
        "CME",
        "MELI",
        "CSX",
        "MNST",
        "ATVI",
        "ORLY",
        "NETS",
        "CDNS",
        "SNPS",
    ]
    nas_com_info_filepath = "./data/nas_com_info.csv"
    all_dataframes = []
    for i in range(5):
        if i != 0:
            if i % 5 == 0:
                time.sleep(60)
        df = com_info(symbols[i])
        all_dataframes.append(df)
    raw_df = pd.concat(all_dataframes, ignore_index=True)
    raw_df.to_csv(nas_com_info_filepath, mode="w", index=False, header=False)


def transform():
    nas_com_info_filepath = "./data/nas_com_info.csv"
    raw_df = pd.read_csv(nas_com_info_filepath)  # 저장된 데이터를 다시 DataFrame으로 불러옴
    transformed_df = raw_df
    transformed_df = transformed_df.drop(transformed_df.columns[3], axis=1)
    transformed_df.to_csv(
        "./data/nas_com_info_t.csv", index=False
    )  # 다음 테스크로 데이터를 이동시키기 위해 csv 파일로 저장.


def load():
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ
    nas_com_info_filepath = "./data/nas_com_info_t.csv"
    s3 = boto3.resource(  # s3 연결 객체
        "s3",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"],
    )

    with open(nas_com_info_filepath, "rb") as f:
        s3.Bucket("de-5-2").put_object(Key=f"nas_com_info.csv", Body=f)


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
    file_name = "nas_com_info.csv"

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

    # 새로운 데이터 테이블 생성
    table_name = "nas_com_info"
    create_table_query = f"""
        DROP TABLE IF EXISTS raw_data.{table_name};
        CREATE TABLE raw_data.{table_name} (
            Symbol VARCHAR(100),
            AssetType VARCHAR(100),
            Name VARCHAR(100),
            CIK VARCHAR(100),
            Exchange VARCHAR(100),
            Currency VARCHAR(100),
            Country VARCHAR(100),
            Sector VARCHAR(100),
            Industry VARCHAR(100),
            Address VARCHAR(100),
            FiscalYearEnd VARCHAR(100),
            LatestQuarter VARCHAR(100),
            MarketCapitalization VARCHAR(100),
            EBITDA VARCHAR(100),
            PERatio VARCHAR(100),
            PEGRatio VARCHAR(100),
            BookValue VARCHAR(100),
            DividendPerShare VARCHAR(100),
            DividendYield VARCHAR(100),
            EPS VARCHAR(100),
            RevenuePerShareTTM VARCHAR(100),
            ProfitMargin VARCHAR(100),
            OperatingMarginTTM VARCHAR(100),
            ReturnOnAssetsTTM VARCHAR(100),
            ReturnOnEquityTTM VARCHAR(100),
            RevenueTTM VARCHAR(100),
            GrossProfitTTM VARCHAR(100),
            DilutedEPSTTM VARCHAR(100),
            QuarterlyEarningsGrowthYOY VARCHAR(100),
            QuarterlyRevenueGrowthYOY VARCHAR(100),
            AnalystTargetPrice VARCHAR(100),
            TrailingPE VARCHAR(100),
            ForwardPE VARCHAR(100),
            PriceToSalesRatioTTM VARCHAR(100),
            PriceToBookRatio VARCHAR(100),
            EVToRevenue VARCHAR(100),
            EVToEBITDA VARCHAR(100),
            Beta VARCHAR(100),
            Week52High VARCHAR(100),
            Week52Low VARCHAR(100),
            Day50MovingAverage VARCHAR(100),
            Day200MovingAverage VARCHAR(100),
            SharesOutstanding VARCHAR(100),
            DividendDate VARCHAR(100),
            ExDividendDate VARCHAR(100)
        );"""

    cur = connection.cursor()
    cur.execute(create_table_query)
    connection.commit()

    # copy_expert를 사용하여 메모리에서 바로 Postgres로 데이터 업로드
    copy_sql = """
    COPY raw_data.nas_com_info FROM stdin WITH CSV DELIMITER ','
    """
    cur.copy_expert(sql=copy_sql, file=f)
    connection.commit()

    # 데이터베이스 연결 종료
    cur.close()
    connection.close()
