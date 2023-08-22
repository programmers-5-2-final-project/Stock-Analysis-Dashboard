doc_md = """
### krx_partition_of_stock_by_code dag

#### 전체적인 흐름

raw_data.krx_stock -> analytics.krx_stock_{code}
postgres partition으로 분리, 의존성도 있고 메모리 자원도 아낌, 즉 데이터를 효율적으로 관리.

1. raw_data.krx_stock에서 code list 추출
2. raw_data.krx_stock.* 로 세팅된 analytics.krx_partition_of_stock_by_code 테이블을 생성
3. 생성한 테이블의 code key의 code_list로 파티션 analytics.krx_stock_{code} 테이블 모두 생성
4. raw_data.krx_stock  테이블을 analytics.krx_partition_of_stock_by_code 에 insert



#### Dag 특징
1. PythonOperator만 사용. 다음 스프린트때는 다른 오퍼레이터(ex. S3HookOperator)도 사용할 예정입니다.
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.
3. query 문으로 작업

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
from sqlalchemy import create_engine, text
import FinanceDataReader as fdr
from dotenv import dotenv_values
import logging


task_logger = logging.getLogger("airflow.task")


@task
def extract_code_list():
    resultproxy = engine.execute(text("SELECT DISTINCT code FROM raw_data.krx_stock;"))
    code_list = []
    for rowproxy in resultproxy:
        for _, code in rowproxy.items():
            code_list.append(code)
    return code_list


@task
def create_table(code_list):
    for code in code_list:
        task_logger.info(f"delete table previous analytics.krx_stock_{code}")
        engine.execute(
            text(
                f"""
                    DROP TABLE IF EXISTS analytics.krx_stock_{code};
                        """
            )
        )

    engine.execute(
        text(
            """
                    DROP TABLE IF EXISTS analytics.krx_partition_of_stock_by_code;
                        """
        )
    )

    task_logger.info("create table analytics.krx_partition_of_stock_by_code")
    engine.execute(
        text(
            """
                CREATE TABLE analytics.krx_partition_of_stock_by_code(
                       date TIMESTAMP,
                       open INTEGER,
                       high INTEGER,
                       low INTEGER,
                       close INTEGER,
                       volume INTEGER,
                       code VARCHAR(40),
                       CONSTRAINT PK_krx_partition_of_stock_by_code PRIMARY KEY(date, code)
                ) PARTITION BY LIST(code);
                       """
        )
    )
    return code_list


@task
def create_partitioned_tables(code_list):
    for code in code_list:
        task_logger.info(f"create partitioned analytics.krx_stock_{code}")
        engine.execute(
            text(
                f"""
                    CREATE TABLE analytics.krx_stock_{code}
                        PARTITION OF analytics.krx_partition_of_stock_by_code
                        FOR VALUES IN ('{code}');
                        """
            )
        )

    return True


@task
def insert_into_table(_):
    task_logger.info("insert into table from raw_data.krx_stock")
    engine.execute(
        text(
            """
                    INSERT INTO analytics.krx_partition_of_stock_by_code
                        SELECT * FROM raw_data.krx_stock;
                        """
        )
    )
    return True


with DAG(
    dag_id="krx_partition_of_stock_by_code2",
    doc_md=doc_md,
    schedule="0 0 * * *",  # UTC기준 하루단위. 자정에 실행되는 걸로 알고 있습니다.
    start_date=days_ago(1),  # 하루 전으로 설정해서 airflow webserver에서 바로 실행시키도록 했습니다.
) as dag:
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ

    # connect RDS
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

    insert_into_table(create_partitioned_tables(create_table(extract_code_list())))

    # close RDS
    conn.close()
    engine.dispose()
