from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import dotenv_values
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import NoCredentialsError
from io import StringIO, BytesIO
import boto3
import os
import sys
import pandas as pd
import logging


'''
✨전체적인 흐름

1. NASDAQ에 상장되어 있는 현재 기업의 심볼을 추출
2.1 모든 기업의 심볼을 추출한 후
2.2 모든 기업의 심볼을 기준으로 주식데이터 추출해서 csv파일로 저장
2.3 csv파일을 S3에 적재
2.3 S3에 적재한 csv파일을 RDS(raw_data)에 COPY
2.4 RDS(raw_data)에 저장된 하나의 데이터 테이블을 RDS(analysis_data)에 기업별로 분리하여 저장


✨Dag 특징
1. PythonOperator만 사용. 다음 스프린트때는 다른 오퍼레이터(ex. S3HookOperator)도 사용할 예정입니다.
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.
3. task간 데이터 이동은 .csv로 로컬에 저장한 후 다시 DataFrame으로 불러오는 방식입니다. 단 krx_list는 xcom방식입니다.
4. 하루단위로 실행. api 자체가 2003.01.01 부터 추출할 수 있어서 start_date를 하루전으로 설정했습니다.

'''

task_logger = logging.getLogger("airflow.task") # airflow log에 남기기 위한 사전작업. dag 디버깅하실때, 사용하시면 좋아요!

@task
def extract_nas_list():  # NASDAQ 상장되어 있는 현재 기업의 심볼을 추출 테스크
    task_logger.info("Extract_nas_list")
    sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    from api import nas_list # nas_list api 모듈
    nas_list_df = nas_list.extract()
    nas_list_df.to_csv("./data/nas_list.csv", index=False, encoding="utf-8-sig") # 다음 테스크로 데이터를 이동시키기 위해 csv 파일로 저장
    nas_list = nas_list_df["Code"].tolist()
    return nas_list


@task
def extract_nas_stock(): # 모든 주식데이터 추출 테스크
    sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    from api import nas_stock # nas_stock api 모듈
    
    return nas_stock.extract()


# 모든 주식데이터 S3에 적재 테스크
@task
def load_nas_stock_to_s3():
    task_logger.info("Load nas_stock_to_s3_from_csv")
    sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    from api import nas_to_s3 # nas_to_s3 api 모듈
    
    return nas_to_s3.load()


# S3에 적재한 주식데이터를 RDS(DW)에 COPY 테스크
@task
def load_nas_stock_to_rds_from_s3():
    task_logger.info("Load nas_stock_to_rds_from_s3")
    sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    from api import nas_to_rds # nas_to_rds api 모듈
    nas_to_rds.rds_list()
    nas_to_rds.rds_stock()
    return


with DAG(
    dag_id="nas_dag1", # dag 이름. 코드를 변경하시고 저장하시면 airflow webserver와 동기화 되는데, dag_id가 같으면 dag를 다시 실행할 수 없어, 코드를 변경하시고 dag이름을 임의로 바꾸신후 테스트하시면 편해요. 저는 dag1, dag2, dag3, ... 방식으로 했습니다.
    schedule = '0 0 * * *', # UTC기준 하루단위. 자정에 실행되는 걸로 알고 있습니다.
    start_date = days_ago(1) # 하루 전으로 설정해서 airflow webserver에서 바로 실행시키도록 했습니다.
) as dag:
    pass
    # krx_list = extract_krx_list() # KRX(코스피, 코스닥, 코스넷)에 상장되어 있는 현재 기업의 심볼을 추출 테스크 실행
    # load_krx_stock_to_rds_from_s3(load_krx_stock_to_s3(transform_krx_stock((extract_krx_stock(krx_list)))))





# @task
# def transform_nas_stock(nas_list): # 기업 단위로 추출한 주식데이터 전처리 테스크
#     for symbol in nas_list:
#         task_logger.info(f"Transform nas_stock_{symbol}")
#         raw_df = pd.read_csv(f"./tmp/nas_stock_{symbol}.csv") # 저장된 데이터를 다시 DataFrame으로 불러옴
#         transformed_df = raw_df.drop(columns=["Change"]) # Change 컬럼 제거
#         transformed_df = transformed_df.dropna() # Nan값이 있는 행은 제거 -> 날짜가 중간중간 빔 -> 다음 스프린트때 수정필요
#         transformed_df.to_csv(f"./data/krx_stock_{symbol}.csv", index=False) # 다음 테스크로 데이터를 이동시키기 위해 csv 파일로 저장.
#     return nas_list

    # #RDS에 접속
    # connection_uri = "postgresql://{}:{}@{}:{}/postgres".format(
    #     CONFIG["POSTGRES_USER"],
    #     CONFIG["POSTGRES_PASSWORD"],
    #     CONFIG["POSTGRES_HOST"],
    #     CONFIG["POSTGRES_PORT"],
    # ) 
    # engine = create_engine(connection_uri, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
    # conn = engine.connect()
