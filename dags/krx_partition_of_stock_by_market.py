'''
✨전체적인 흐름

raw_data.krx_list -> analytics.kospi_list, analytics.kosdaq_list, analytics.konex_list
postgres partition으로 분리, 의존성도 있고 메모리 자원도 아낌, 즉 데이터를 효율적으로 관리.

1. raw_data.krx_stock.*, raw_data.krx_list.name, market, marcap 로 세팅된 analytics.krx_partition_of_stock_by_market 테이블 선언
2. 선언한 테이블의 market key의 kospi, (kosdaq, kosdaq global), konex 로 파티션 analytics.kospi_stock, analytics.kosdaq_stock, analytics.konex_stock 테이블 선언
3. raw_data.krx_stock 과 raw_data.krx_list를 inner join한 테이블을 analytics.krx_partition_of_stock_by_market 에 insert



✨Dag 특징
1. PythonOperator만 사용. 다음 스프린트때는 다른 오퍼레이터(ex. S3HookOperator)도 사용할 예정입니다.
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.
3. query 문으로 작업

✨Domain 특징
1. http://data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd?baseName=krx.mdc.i18n.component&key=B128.bld
2. cols_map = {'ISU_SRT_CD':'Code', 'ISU_ABBRV':'Name', 
                'TDD_CLSPRC':'Close', 'SECT_TP_NM': 'Dept', 'FLUC_TP_CD':'ChangeCode', 
                'CMPPREVDD_PRC':'Changes', 'FLUC_RT':'ChagesRatio', 'ACC_TRDVOL':'Volume', 
                'ACC_TRDVAL':'Amount', 'TDD_OPNPRC':'Open', 'TDD_HGPRC':'High', 'TDD_LWPRC':'Low',
                'MKTCAP':'Marcap', 'LIST_SHRS':'Stocks', 'MKT_NM':'Market', 'MKT_ID': 'MarketId' }
'''

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
    engine.execute(text("""
                DROP TABLE IF EXISTS analytics.kospi_stock;
                DROP TABLE IF EXISTS analytics.kosdaq_stock;
                DROP TABLE IF EXISTS analytics.konex_stock;
                DROP TABLE IF EXISTS analytics.krx_partition_of_stock_by_market;
                       """))
    task_logger.info("create table analytics.krx_partition_of_stock_by_market")
    engine.execute(text("""
                CREATE TABLE analytics.krx_partition_of_stock_by_market(
                       date TIMESTAMP,
                       open INTEGER,
                       high INTEGER,
                       low INTEGER,
                       close INTEGER,
                       volume INTEGER,
                       code VARCHAR(40),
                       name VARCHAR(40),
                       market VARCHAR(40),
                       marcap BIGINT,
                       CONSTRAINT PK_krx_partition_of_stock_by_market PRIMARY KEY(date, code, market)
                ) PARTITION BY LIST(market);
                       """))
    return True

@task
def create_partitioned_tables(_):
    task_logger.info("create_partitioned_analytics.kospi_stock_table")
    engine.execute(text("""
                CREATE TABLE analytics.kospi_stock
                       PARTITION OF analytics.krx_partition_of_stock_by_market
                       FOR VALUES IN ('KOSPI');
                       """))
    
    task_logger.info("create_partitioned_analytics.kosdaq_stock")
    engine.execute(text("""
                CREATE TABLE analytics.kosdaq_stock
                       PARTITION OF analytics.krx_partition_of_stock_by_market
                       FOR VALUES IN ('KOSDAQ', 'KOSDAQ GLOBAL');
                       """))
    
    task_logger.info("create_partitioned_analytics.konex_stock")
    engine.execute(text("""
                CREATE TABLE analytics.konex_stock
                       PARTITION OF analytics.krx_partition_of_stock_by_market
                       FOR VALUES IN ('KONEX');
                       """))
    return True

@task
def insert_into_table(_):
    task_logger.info("insert into table from joined table krx_stock and krx_list")
    engine.execute(text("""
                WITH joined_table_krx_stock_list 
                        AS (SELECT krx_stock.*, krx_list.name, krx_list.market, krx_list.marcap
                                FROM raw_data.krx_stock AS krx_stock
                                INNER JOIN raw_data.krx_list AS krx_list
                                ON krx_stock.code = krx_list.code)
                    INSERT INTO analytics.krx_partition_of_stock_by_market
                        SELECT * FROM joined_table_krx_stock_list;
                        """))
    return True

with DAG(
    dag_id="krx_partition_of_stock_by_market6", 
    schedule = '0 0 * * *', # UTC기준 하루단위. 자정에 실행되는 걸로 알고 있습니다.
    start_date = days_ago(1) # 하루 전으로 설정해서 airflow webserver에서 바로 실행시키도록 했습니다.
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
        CONFIG["POSTGRES_DB"]
    ) 
    engine = create_engine(connection_uri, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
    conn = engine.connect()

    insert_into_table(create_partitioned_tables(create_table()))

    # close RDS
    conn.close()
    engine.dispose()