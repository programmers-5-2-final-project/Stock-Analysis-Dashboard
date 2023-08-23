doc_md = """
### krx_list_dag

#### 전체적인 흐름

1. KRX(코스피, 코스닥, 코스넷)에 상장된 기업들 추출
2. 추출한 데이터 처리. Code or Name가 Nan인 행렬은 삭제 처리
3. .csv 파일을 s3에 적재
4. s3 -> rds에 적재

#### Dag 특징
1. PythonOperator만 사용. 다음 스프린트때는 다른 오퍼레이터(ex. S3HookOperator)도 사용할 예정입니다.
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.
3. task간 데이터 이동은 .csv로 로컬에 저장한 후 다시 DataFrame으로 불러오는 방식입니다.

#### Domain 특징
1. http://data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd?baseName=krx.mdc.i18n.component&key=B128.bld
2. cols_map = {'ISU_SRT_CD':'Code', 'ISU_ABBRV':'Name', 
                'TDD_CLSPRC':'Close', 'SECT_TP_NM': 'Dept', 'FLUC_TP_CD':'ChangeCode', 
                'CMPPREVDD_PRC':'Changes', 'FLUC_RT':'ChagesRatio', 'ACC_TRDVOL':'Volume', 
                'ACC_TRDVAL':'Amount', 'TDD_OPNPRC':'Open', 'TDD_HGPRC':'High', 'TDD_LWPRC':'Low',
                'MKTCAP':'Marcap', 'LIST_SHRS':'Stocks', 'MKT_NM':'Market', 'MKT_ID': 'MarketId' }

"""

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import boto3
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import dotenv_values
import logging

from dags.ETL_dags.common_package.extract import Extract

task_logger = logging.getLogger("airflow.task")


@task
def extract_krx_list():
    """
    input: None | output: KRX 상장회사(발행회사)목록을 tmp/krx_list.csv에 저장해서 전달
    """
    task_logger.info("Extract_krx_list")
    extract_krx = Extract("KRX")
    raw_df = (
        extract_krx.values_of_listed_companies()
    )  # input: None | output: KRX 상장회사(발행회사)목록 (가격 중심, 주식 종목), pd.DataFrame(columns=["Code","ISU_CD","Name","Market","Dept","Close","ChangeCode","Changes","ChangesRatio","Open","High","Low","Volume","Amount","Marcap","Stocks","MarketId"])
    new_columns = [
        "Code",
        "ISU_CD",
        "Name",
        "Market",
        "Dept",
        "Close",
        "ChangeCode",
        "Changes",
        "ChangesRatio",
        "Open",
        "High",
        "Low",
        "Volume",
        "Amount",
        "Marcap",
        "Stocks",
        "MarketId",
    ]
    df = pd.DataFrame(columns=new_columns)
    df.to_csv("./tmp/krx_list.csv", index=False, encoding="utf-8-sig")
    raw_df.to_csv(
        "./tmp/krx_list.csv", mode="a", index=False, header=False, encoding="utf-8-sig"
    )
    return True


@task
def transform_krx_list(_):
    """
    input: ./tmp/krx_list.csv | output: [Code, Name] 컬럼이 NaN 값이면 행을 삭제한 KRX 상장회사(발행회사)목록을 data/krx_list.csv에 저장해서 전달
    """
    task_logger.info("Transform krx_list")
    raw_df = pd.read_csv("./tmp/krx_list.csv")
    transformed_df = raw_df.dropna(subset=["Code", "Name"])
    new_columns = [
        "Code",
        "ISU_CD",
        "Name",
        "Market",
        "Dept",
        "Close",
        "ChangeCode",
        "Changes",
        "ChangesRatio",
        "Open",
        "High",
        "Low",
        "Volume",
        "Amount",
        "Marcap",
        "Stocks",
        "MarketId",
    ]
    df = pd.DataFrame(columns=new_columns)
    df.to_csv("./data/krx_list.csv", index=False, encoding="utf-8-sig")
    transformed_df.to_csv(
        "./data/krx_list.csv", mode="a", index=False, header=False, encoding="utf-8-sig"
    )
    return True


@task
def load_krx_list_to_s3(_):
    """
    input: ./data/krx_list.csv | output: s3에 krx_list.csv 오브젝트로 저장
    """
    task_logger.info("Load_krx_list_to_s3")
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"],
    )
    # delete_s3bucket_objects(s3, symbol) # Full refresh 방식이어서 먼저 S3에 저장된 객체를 삭제. 삭제 권한이 없어 주석처리
    krx_list_data = open("./data/krx_list.csv", "rb")
    s3.Bucket("de-5-2").put_object(Key="krx_list.csv", Body=krx_list_data)
    return True


@task
def load_krx_list_to_rds_from_s3(_):
    """
    input: s3오브젝트인 krx_list.csv | output: rds에 raw_data.krx_list table
    """
    task_logger.info("Load krx_list_to_rds_from_s3")
    task_logger.info("Creating the table raw_data.krx_list")
    engine.execute(
        text(
            """
                DROP TABLE IF EXISTS raw_data.krx_list;
                CREATE TABLE raw_data.krx_list(
                Code VARCHAR(40) PRIMARY KEY,
                ISU_CD VARCHAR(40),
                Name VARCHAR(40) NOT NULL,
                Market VARCHAR(40),
                Dept VARCHAR(40),
                Close VARCHAR(40),
                ChangeCode VARCHAR(40),
                Changes VARCHAR(40),
                ChangesRatio VARCHAR(40),
                Open VARCHAR(40),
                High VARCHAR(40),
                Low VARCHAR(40),
                Volume VARCHAR(40),
                Amount VARCHAR(40),
                Marcap VARCHAR(40),
                Stocks VARCHAR(40),
                MarketId VARCHAR(40)
            );"""
        )
    )
    task_logger.info(
        "Importing krx_list.csv data from Amazon S3 to RDS for PostgreSQL DB instance"
    )
    engine.execute(
        text(
            f"""
                SELECT aws_s3.table_import_from_s3(
                'raw_data.krx_list', '', '(format csv)',
                aws_commons.create_s3_uri('de-5-2', 'krx_list.csv', 'ap-northeast-2'),
                aws_commons.create_aws_credentials('{CONFIG["AWS_ACCESS_KEY_ID"]}', '{CONFIG["AWS_SECRET_ACCESS_KEY"]}', '')    
            );"""
        )
    )
    task_logger.info("Converting column types")
    engine.execute(text("DELETE FROM raw_data.krx_list WHERE code like '%Code%';"))
    engine.execute(
        text(
            """
                ALTER TABLE raw_data.krx_list
                    ALTER COLUMN Close TYPE INTEGER USING Close::INTEGER,
                    ALTER COLUMN ChangeCode TYPE INTEGER USING ChangeCode::INTEGER,
                    ALTER COLUMN Changes TYPE INTEGER USING Changes::INTEGER,
                    ALTER COLUMN ChangesRatio TYPE FLOAT USING ChangesRatio::FLOAT,
                    ALTER COLUMN Open TYPE INTEGER USING Open::INTEGER,
                    ALTER COLUMN High TYPE INTEGER USING High::INTEGER,
                    ALTER COLUMN Low TYPE INTEGER USING Low::INTEGER,
                    ALTER COLUMN Volume TYPE INTEGER USING Volume::INTEGER,
                    ALTER COLUMN Amount TYPE BIGINT USING Amount::BIGINT,
                    ALTER COLUMN Marcap TYPE BIGINT USING Marcap::BIGINT,
                    ALTER COLUMN Stocks TYPE BIGINT USING Stocks::BIGINT;
                """
        )
    )
    return True


with DAG(
    dag_id="krx_list_dag13",
    doc_md=doc_md,
    schedule="0 0 * * *",
    start_date=days_ago(1),
) as dag:
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ

    connection_uri = "postgresql://{}:{}@{}:{}/{}".format(
        CONFIG["POSTGRES_USER"],
        CONFIG["POSTGRES_PASSWORD"],
        CONFIG["POSTGRES_HOST"],
        CONFIG["POSTGRES_PORT"],
        CONFIG["POSTGRES_DB"],
    )
    engine = create_engine(
        connection_uri, pool_pre_ping=True, isolation_level="AUTOCOMMIT"
    )
    conn = engine.connect()

    load_krx_list_to_rds_from_s3(
        load_krx_list_to_s3(transform_krx_list(extract_krx_list()))
    )

    conn.close()
    engine.dispose()
