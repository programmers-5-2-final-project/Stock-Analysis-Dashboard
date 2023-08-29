doc_md = """
### krx_stock dag

#### 전체적인 흐름

1. KRX(코스피, 코스닥, 코스넷)에 상장되어 있는 기업의 심볼 가져오기. raw_data.krx_list에서 가져옴
2. 주식데이터 추출
3. 추출한 주식데이터 처리.
4. .csv 파일을 s3에 적재
5. S3->RDS 적재

#### Dag 특징
1. PythonOperator만 사용. 다음 스프린트때는 다른 오퍼레이터(ex. S3HookOperator)도 사용할 예정입니다.
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.
3. task간 데이터 이동은 .csv로 로컬에 저장한 후 다시 DataFrame으로 불러오는 방식입니다. 단 krx_list는 xcom방식입니다.
4. 하루단위로 실행. api 자체가 2003.01.02 부터 추출할 수 있어서 start_date를 하루전으로 설정했습니다.

#### Domain 특징
1. https://fchart.stock.naver.com/sise.nhn?timeframe=day&count=6000&requestType=0&symbol=
2. ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'] + 'Change'
"""

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import logging
from ETL_dags.krx.krx_stock.extract_data import (
    extract_krx_list_data_from_rds,
    extract_all_krx_stock_data,
)
from ETL_dags.krx.krx_stock.transform_data import transform_krx_stock_data
from ETL_dags.krx.krx_stock.load_data_to_s3 import load_krx_stock_data_to_s3
from ETL_dags.krx.krx_stock.load_data_to_rds_from_s3 import (
    load_krx_stock_data_to_rds_from_s3,
)
from ETL_dags.krx.krx_stock.load_data_to_redshift_from_s3 import (
    load_krx_stock_data_to_redshift_from_s3,
)

task_logger = logging.getLogger("airflow.task")


@task
def extract_krx_list():
    task_logger.info("Extract krx list from rds")
    krx_list = extract_krx_list_data_from_rds(task_logger)

    return krx_list


@task
def extract_krx_stock(krx_list):
    task_logger.info("Extract krx stock")
    extract_all_krx_stock_data(krx_list)

    return True


@task
def transform_krx_stock(_):
    task_logger.info("Transform krx stock")
    transform_krx_stock_data(task_logger)

    return True


@task
def load_krx_stock_to_s3(_):
    task_logger.info("Load_krx_stock_to_s3")
    load_krx_stock_data_to_s3(task_logger)

    return True


@task
def load_krx_stock_to_dw_from_s3(_):
    task_logger.info("Load krx_stock_to_rds_from_s3")
    load_krx_stock_data_to_rds_from_s3(task_logger)
    load_krx_stock_data_to_redshift_from_s3(task_logger)

    return True


with DAG(
    dag_id="krx_stock_dag31",
    doc_md=doc_md,
    schedule="0 0 * * *",
    start_date=days_ago(1),
) as dag:
    # krx_list = extract_krx_list()
    # load_krx_stock_to_dw_from_s3(
    #     load_krx_stock_to_s3(transform_krx_stock((extract_krx_stock(krx_list))))
    # )
    load_krx_stock_to_dw_from_s3(True)
