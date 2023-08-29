# snp_dag2_com_info.py
from airflow import DAG
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from dotenv import dotenv_values
from io import StringIO
import pandas as pd
import os
import sys
import logging
import boto3
import os
import sys
import FinanceDataReader as fdr
import logging
import psycopg2
import time
from ETL_dags.snp500 import snp_co_info_
from plugins import slack

task_logger = logging.getLogger(
    "airflow.task"
)  # airflow log에 남기기 위한 사전작업. dag 디버깅하실때, 사용하시면 좋아요!


@task  # _extract_snp_list 메소드가 선행되어야 함
def extract_snp_co_info():  # 기업 단위로 주식데이터 추출 테스크
    task_logger.info("Extract_snp_stock")
    # sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    # from api import snp_com_info  # snp_fetch_capital 모듈

    snp_co_info_.extract()
    return


@task
def load_snp_co_info_to_s3():  # 기업 단위로 주식데이터 S3에 적재 테스크
    task_logger.info(f"Load_snp_com_info_to_s3")
    # sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    # from api import snp_com_info

    snp_co_info_.load()
    return True


@task
def load_snp_stock_to_rds_from_s3():  # 기업 단위로 S3에 적재한 주식데이터를 RDS(DW)에 COPY 테스크
    task_logger.info(f"Load_snp_com_info_to_rds_from_s3")
    # sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    # from api import snp_com_info

    snp_co_info_.rds()
    return True


with DAG(
    dag_id="snp_dag_co_info1",  # dag 이름. 코드를 변경하시고 저장하시면 airflow webserver와 동기화 되는데, dag_id가 같으면 dag를 다시 실행할 수 없어, 코드를 변경하시고 dag이름을 임의로 바꾸신후 테스트하시면 편해요. 저는 dag1, dag2, dag3, ... 방식으로 했습니다.
    schedule="0 0 * * *",  # UTC기준 하루단위. 자정에 실행되는 걸로 알고 있습니다.
    start_date=days_ago(1),  # 하루 전으로 설정해서 airflow webserver에서 바로 실행시키도록 했습니다.
    catchup=False,  # 과거의 task를 실행할지 여부. False로 설정하면, 과거의 task는 실행되지 않습니다.
    default_args={
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:
    (
        extract_snp_co_info()
        >> load_snp_co_info_to_s3()
        >> load_snp_stock_to_rds_from_s3()
    )  # dag의 task를 순서대로 연결해줍니다. >> 를 사용하시면 됩니다.
