"""
✨전체적인 흐름

raw_data.krx_list & raw_data.krx_co_info -> analytics.krx_partition_of_list_by_market
postgres partition으로 분리, 의존성도 있고 메모리 자원도 아낌, 즉 데이터를 효율적으로 관리.

1. raw_data.krx_list.*, raw_data.krx_co_info.* 로 세팅된 analytics.krx_partition_of_list_by_market 테이블 선언
2. 생성한 테이블의 market key의 kospi, kosdaq, konex 로 파티션 테이블 생성
3. raw_data.krx_list raw_data.krx_co_info를 조인한 테이블로 부터 analytics.krx_partition_of_list_by_market에 insert 



✨Dag 특징
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
def create_table():
    task_logger.info("delete table previous tables")
    engine.execute(
        text(
            """
                DROP TABLE IF EXISTS analytics.kospi_list;
                DROP TABLE IF EXISTS analytics.kosdaq_list;
                DROP TABLE IF EXISTS analytics.konex_list;
                DROP TABLE IF EXISTS analytics.krx_partition_of_list_by_market;
                       """
        )
    )
    task_logger.info("create table analytics.krx_partition_of_list_by_market")
    engine.execute(
        text(
            """
                CREATE TABLE analytics.krx_partition_of_list_by_market(
                        Code VARCHAR(40),
                        ISU_CD VARCHAR(40),
                        Name VARCHAR(40) NOT NULL,
                        Market VARCHAR(40),
                        Dept VARCHAR(40),
                        Close INTEGER,
                        ChangeCode INTEGER,
                        Changes INTEGER,
                        ChangesRatio FLOAT,
                        Open INTEGER,
                        High INTEGER,
                        Low INTEGER,
                        Volume INTEGER,
                        Amount BIGINT,
                        Marcap BIGINT,
                        Stocks BIGINT,
                        MarketId VARCHAR(40),
                        sector VARCHAR(40),
                        industry VARCHAR(300),
                        listingdate TIMESTAMP,
                        settlemonth VARCHAR(40),
                        representative VARCHAR(300),
                        homepage VARCHAR(300),
                        region VARCHAR(40),
                        CONSTRAINT PK_krx_partition_of_list_by_market PRIMARY KEY(code, market)
                ) PARTITION BY LIST(market);
                       """
        )
    )
    return True


@task
def create_partitioned_tables(_):
    task_logger.info("create_partitioned_analytics.kospi_list_table")
    engine.execute(
        text(
            """
                CREATE TABLE analytics.kospi_list
                       PARTITION OF analytics.krx_partition_of_list_by_market
                       FOR VALUES IN ('KOSPI');
                       """
        )
    )

    task_logger.info("create_partitioned_analytics.kosdaq_list")
    engine.execute(
        text(
            """
                CREATE TABLE analytics.kosdaq_list
                       PARTITION OF analytics.krx_partition_of_list_by_market
                       FOR VALUES IN ('KOSDAQ', 'KOSDAQ GLOBAL');
                       """
        )
    )

    task_logger.info("create_partitioned_analytics.konex_list")
    engine.execute(
        text(
            """
                CREATE TABLE analytics.konex_list
                       PARTITION OF analytics.krx_partition_of_list_by_market
                       FOR VALUES IN ('KONEX');
                       """
        )
    )
    return True


@task
def insert_into_table(_):
    task_logger.info("insert into table from raw_data.krx_list")
    engine.execute(
        text(
            """
                WITH joined_table_krx_list_co_info
                        AS (SELECT krx_list.*, 
                                krx_co_info.sector, 
                                krx_co_info.industry, 
                                krx_co_info.listingdate, 
                                krx_co_info.settlemonth, 
                                krx_co_info.representative, 
                                krx_co_info.homepage, 
                                krx_co_info.region
                                FROM raw_data.krx_list AS krx_list
                                LEFT JOIN raw_data.krx_co_info AS krx_co_info
                                ON krx_list.code = krx_co_info.code)
                    INSERT INTO analytics.krx_partition_of_list_by_market
                        SELECT * FROM joined_table_krx_list_co_info;
                        """
        )
    )
    return True


with DAG(
    dag_id="krx_partition_of_list_by_market4",
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

    insert_into_table(create_partitioned_tables(create_table()))

    # close RDS
    conn.close()
    engine.dispose()
