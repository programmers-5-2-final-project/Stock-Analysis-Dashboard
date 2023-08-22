doc_md = """
### krx_dag

#### 전체적인 흐름

1. KRX(코스피, 코스닥, 코스넷)에 상장되어 있는 현재 기업의 심볼을 추출
2.1 기업 단위로 주식데이터 추출 후
2.2 기업 단위로 추출한 주식데이터 전처리
2.3 기업 단위로 주식데이터 S3에 적재
2.4 기업 단위로 S3에 적재한 주식데이터를 RDS(DW)에 COPY

#### Dag 특징
1. PythonOperator만 사용. 다음 스프린트때는 다른 오퍼레이터(ex. S3HookOperator)도 사용할 예정입니다.
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.
3. task간 데이터 이동은 .csv로 로컬에 저장한 후 다시 DataFrame으로 불러오는 방식입니다. 단 krx_list는 xcom방식입니다.
4. 하루단위로 실행. api 자체가 2003.01.01 부터 추출할 수 있어서 start_date를 하루전으로 설정했습니다.

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
import boto3
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import FinanceDataReader as fdr
from dotenv import dotenv_values
import logging


task_logger = logging.getLogger(
    "airflow.task"
)  # airflow log에 남기기 위한 사전작업. dag 디버깅하실때, 사용하시면 좋아요!


def delete_s3bucket_objects(s3, symbol):  # S3에 저장된 객체를 삭제하는 메서드
    response = s3.delete_object(Bucket="de-5-2", Key=f"krx_stock_{symbol}.csv")
    if response["DeleteMarker"]:
        task_logger.info(f"Succeed delete krx_stock_{symbol}.csv ")
    else:
        task_logger.info(f"Failed delete krx_stock_{symbol}.csv ")


@task
def extract_krx_list():  # KRX(코스피, 코스닥, 코스넷)에 상장되어 있는 현재 기업의 심볼을 추출 테스크
    task_logger.info("Extract_krx_list")
    sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    from dags.ETL_dags.common_package import krx_list  # krx_list api 모듈

    krx_list_df = krx_list.extract()
    krx_list_df.to_csv(
        "./data/krx_list.csv", index=False, encoding="utf-8-sig"
    )  # 다음 테스크로 데이터를 이동시키기 위해 csv 파일로 저장

    return krx_list_df["Code"].tolist()


@task
def extract_krx_stock(krx_list):  # 기업 단위로 주식데이터 추출 테스크
    for symbol in krx_list:
        task_logger.info(f"Extract krx_stock_{symbol}")
        raw_df = fdr.DataReader(symbol, "2003")
        raw_df.to_csv(
            f"./tmp/krx_stock_{symbol}.csv", index=True
        )  # 다음 테스크로 데이터를 이동시키기 위해 csv 파일로 저장.
    return krx_list


@task
def transform_krx_stock(krx_list):  # 기업 단위로 추출한 주식데이터 전처리 테스크
    for symbol in krx_list:
        task_logger.info(f"Transform krx_stock_{symbol}")
        raw_df = pd.read_csv(
            f"./tmp/krx_stock_{symbol}.csv"
        )  # 저장된 데이터를 다시 DataFrame으로 불러옴
        transformed_df = raw_df.drop(columns=["Change"])  # Change 컬럼 제거
        transformed_df = (
            transformed_df.dropna()
        )  # Nan값이 있는 행은 제거 -> 날짜가 중간중간 빔 -> 다음 스프린트때 수정필요
        transformed_df.to_csv(
            f"./data/krx_stock_{symbol}.csv", index=False
        )  # 다음 테스크로 데이터를 이동시키기 위해 csv 파일로 저장.
    return krx_list


@task
def load_krx_stock_to_s3(krx_list):  # 기업 단위로 주식데이터 S3에 적재 테스크
    for symbol in krx_list:
        task_logger.info(f"Load_krx_stock_to_s3_{symbol}")
        s3 = boto3.resource(  # s3 연결 객체
            "s3",
            aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"],
        )
        # delete_s3bucket_objects(s3, symbol) # Full refresh 방식이어서 먼저 S3에 저장된 객체를 삭제. 삭제 권한이 없어 주석처리
        krx_stock_data = open(
            f"./data/krx_stock_{symbol}.csv", "rb"
        )  # 저장된 데이터를 다시 DataFrame으로 불러옴
        s3.Bucket("de-5-2").put_object(
            Key=f"krx_stock_{symbol}.csv", Body=krx_stock_data
        )  # S3 버킷에 저장
    return krx_list


@task
def load_krx_stock_to_rds_from_s3(krx_list):  # 기업 단위로 S3에 적재한 주식데이터를 RDS(DW)에 COPY 테스크
    for symbol in krx_list:
        task_logger.info(f"Load krx_stock_to_rds_from_s3_{symbol}")
        # task_logger.info("Installing the aws_s3 extension")
        # engine.execute("CREATE EXTENSION aws_s3 CASCADE;") # RDS에 aws_s3 extension 추가. 처음에만 추가하면 돼서 주석처리
        task_logger.info(f"Creating the table krx_stock_{symbol}")
        engine.execute(
            f"""
                    DROP TABLE IF EXISTS krx_stock_{symbol};
                    CREATE TABLE krx_stock_{symbol}(
                    Date VARCHAR(40),
                    Open VARCHAR(40),
                    High VARCHAR(40),
                    Low VARCHAR(40),
                    Close VARCHAR(40),
                    Volume VARCHAR(40)
                    );"""
        )  # RDS에 기업단위로 테이블 생성 쿼리.
        task_logger.info(
            f"Importing krx_stock_{symbol}.csv data from Amazon S3 to RDS for PostgreSQL DB instance"
        )
        engine.execute(
            f"""
                    SELECT aws_s3.table_import_from_s3(
                    'krx_stock_{symbol}', 'Date,Open,High,Low,Close,Volume', '(format csv)',
                    aws_commons.create_s3_uri('de-5-2', 'krx_stock_{symbol}.csv', 'ap-northeast-2'),
                    aws_commons.create_aws_credentials('{CONFIG["AWS_ACCESS_KEY_ID"]}', '{CONFIG["AWS_SECRET_ACCESS_KEY"]}', '')    
                    );"""
        )  # S3에서 RDS로 복사하는 쿼리. 자세한 정보는 https://docs.aws.amazon.com/ko_kr/AmazonRDS/latest/UserGuide/USER_PostgreSQL.S3Import.html#aws_s3.table_import_from_s3
    return True


with DAG(
    dag_id="krx_dag41",  # dag 이름. 코드를 변경하시고 저장하시면 airflow webserver와 동기화 되는데, dag_id가 같으면 dag를 다시 실행할 수 없어, 코드를 변경하시고 dag이름을 임의로 바꾸신후 테스트하시면 편해요. 저는 dag1, dag2, dag3, ... 방식으로 했습니다.
    doc_md=doc_md,
    schedule="0 0 * * *",  # UTC기준 하루단위. 자정에 실행되는 걸로 알고 있습니다.
    start_date=days_ago(1),  # 하루 전으로 설정해서 airflow webserver에서 바로 실행시키도록 했습니다.
) as dag:
    CONFIG = dotenv_values(".env")  # .env 파일에 숨겨진 값(AWS ACCESS KEY)을 사용하기 위함.
    if not CONFIG:
        CONFIG = os.environ

    # RDS에 접속
    connection_uri = "postgresql://{}:{}@{}:{}/postgres".format(
        CONFIG["POSTGRES_USER"],
        CONFIG["POSTGRES_PASSWORD"],
        CONFIG["POSTGRES_HOST"],
        CONFIG["POSTGRES_PORT"],
    )
    engine = create_engine(
        connection_uri, pool_pre_ping=True, isolation_level="AUTOCOMMIT"
    )
    conn = engine.connect()

    krx_list = extract_krx_list()  # KRX(코스피, 코스닥, 코스넷)에 상장되어 있는 현재 기업의 심볼을 추출 테스크 실행
    load_krx_stock_to_rds_from_s3(
        load_krx_stock_to_s3(transform_krx_stock((extract_krx_stock(krx_list))))
    )

    # RDS close
    conn.close()
    engine.dispose()
