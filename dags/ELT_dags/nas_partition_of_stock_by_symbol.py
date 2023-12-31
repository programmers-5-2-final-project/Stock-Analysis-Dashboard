# nas_partition_of_stock_by_symbol.py
doc_md = """
### nas_partition_of_stock_by_symbol dag

#### 전체적인 흐름

raw_data.nas_stock -> analytics.nas_stock_{symbol}
postgres partition으로 분리, 의존성도 있고 메모리 자원도 아낌, 즉 데이터를 효율적으로 관리.

1. raw_data.nas_stock에서 symbol list 추출
2. raw_data.nas_stock.* 로 세팅된 analytics.nas_partition_of_stock_by_symbol 테이블을 생성
3. 생성한 테이블의 symbol key의 symbol_list로 파티션 analytics.nas_stock_{symbol} 테이블 모두 생성
4. raw_data.nas_stock  테이블을 analytics.nas_partition_of_stock_by_symbol 에 insert



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
def extract_symbol_list():
    resultproxy = engine.execute(
        text("SELECT DISTINCT symbol FROM raw_data.nas_stock;")
    )
    symbol_list = []
    for rowproxy in resultproxy:
        for _, symbol in rowproxy.items():
            symbol_list.append(symbol)
    return symbol_list


@task
def create_table(symbol_list):
    engine.execute(
        text(
            """
                    DROP TABLE IF EXISTS analytics.nas_partition_of_stock_by_symbol;
                        """
        )
    )

    print("create table analytics.nas_partition_of_stock_by_symbol")
    engine.execute(
        text(
            """
                CREATE TABLE analytics.nas_partition_of_stock_by_symbol(
                        Date Date,
                        Open FLOAT,
                        High FLOAT,
                        Low FLOAT,
                        Close FLOAT,
                        Adj_Close FLOAT,
                        Volume FLOAT,
                        Symbol VARCHAR(40),
                        CONSTRAINT PK_nas_partition_of_stock_by_symbol PRIMARY KEY(date, symbol)
                ) PARTITION BY LIST(symbol);
            """
        )
    )
    return symbol_list


@task
def create_partitioned_tables(symbol_list):
    for symbol in symbol_list:
        print(f"create partitioned analytics.nas_stock_{symbol}")
        engine.execute(
            text(
                f"""
                    CREATE TABLE analytics.nas_stock_{symbol}
                        PARTITION OF analytics.nas_partition_of_stock_by_symbol
                        FOR VALUES IN ('{symbol}');
                        """
            )
        )

    return True


@task
def insert_into_table(_):
    print("insert into table from raw_data.nas_stock")
    engine.execute(
        text(
            """
                    INSERT INTO analytics.nas_partition_of_stock_by_symbol
                        SELECT * FROM raw_data.nas_stock;
                        """
        )
    )
    return True


with DAG(
    dag_id="nas_partition_of_stock_by_code",
    doc_md=doc_md,
    schedule="0 3 * * *",  # UTC기준 하루단위. 자정에 실행되는 걸로 알고 있습니다.
    start_date=days_ago(1),  # 하루 전으로 설정해서 airflow webserver에서 바로 실행시키도록 했습니다.
) as dag:
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ

    # connect RDS
    connection_uri = "postgresql://{}:{}@{}:{}/{}".format(
        CONFIG["RDS_USER"],
        CONFIG["RDS_PASSWORD"],
        CONFIG["RDS_HOST"],
        CONFIG["RDS_PORT"],
        CONFIG["RDS_DB"],
    )
    engine = create_engine(
        connection_uri, pool_pre_ping=True, isolation_level="AUTOCOMMIT"
    )
    conn = engine.connect()

    insert_into_table(create_partitioned_tables(create_table(extract_symbol_list())))

    # close RDS
    conn.close()
    engine.dispose()
