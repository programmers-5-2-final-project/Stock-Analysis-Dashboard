#nas_dag.py
from airflow import DAG
from airflow.decorators import task
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from dotenv import dotenv_values
# from concurrent.futures import ThreadPoolExecutor
# from botocore.exceptions import NoCredentialsError
import os
import sys
import logging


task_logger = logging.getLogger("airflow.task") # airflow log에 남기기 위한 사전작업. dag 디버깅하실때, 사용하시면 좋아요!


# NASDAQ 상장되어 있는 현재 기업의 심볼을 추출 테스크
@task
def extract_nas_list():
    task_logger.info("Extract_nas_list")
    sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    from api import nas_list # nas_list api 모듈
    df = nas_list.extract()
    df.to_csv("./data/nas_list.csv", index=False, encoding="utf-8-sig") # 다음 테스크로 데이터를 이동시키기 위해 csv 파일로 저장

    return



# 모든 주식데이터 추출 테스크
@task
def extract_nas_stock():
    sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    from api import nas_stock # nas_stock api 모듈
    nas_stock.extract(task_logger)
    return


# 모든 주식데이터 S3에 적재 테스크
@task
def load_nas_stock_to_s3():
    task_logger.info("Load nas_stock_to_s3_from_csv")
    sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    from api import nas_to_s3 # nas_to_s3 api 모듈
    nas_to_s3.load()
    return


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
    dag_id="nas_dag37", # dag 이름. 코드를 변경하시고 저장하시면 airflow webserver와 동기화 되는데, dag_id가 같으면 dag를 다시 실행할 수 없어, 코드를 변경하시고 dag이름을 임의로 바꾸신후 테스트하시면 편해요. 저는 dag1, dag2, dag3, ... 방식으로 했습니다.
    schedule = '0 0 * * *', # UTC기준 하루단위. 자정에 실행되는 걸로 알고 있습니다.
    start_date = days_ago(1), # 하루 전으로 설정해서 airflow webserver에서 바로 실행시키도록 했습니다.
    catchup=False # 과거의 task를 실행할지 여부. False로 설정하면, 과거의 task는 실행되지 않습니다.
) as dag:
    extract_nas_list() >> extract_nas_stock() >> load_nas_stock_to_s3() >> load_nas_stock_to_rds_from_s3() # dag의 task를 순서대로 연결해줍니다. >> 를 사용하시면 됩니다.