doc_md = """
### krx_co_info_dag

#### 전체적인 흐름

1. KRX(코스피, 코스닥, 코스넷)에 상장된 기업들의 정보 추출
2. 추출한 데이터 처리. Code or Name가 Nan인 행렬은 삭제 처리
3. .csv 파일을 s3에 적재
4. s3 -> rds에 적재

#### Dag 특징
1. PythonOperator만 사용. 다음 스프린트때는 다른 오퍼레이터(ex. S3HookOperator)도 사용할 예정입니다.
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.
3. task간 데이터 이동은 .csv로 로컬에 저장한 후 다시 DataFrame으로 불러오는 방식입니다.

#### Domain 특징
1. http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13
2. cols_ren = {'회사명':'Name', '종목코드':'Code', '업종':'Sector', '주요제품':'Industry', 
                    '상장일':'ListingDate', '결산월':'SettleMonth',  '대표자명':'Representative', 
                    '홈페이지':'HomePage', '지역':'Region', }

"""

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import logging

from ETL_dags.krx.krx_co_info.extract_data import extract_krx_co_info_data
from ETL_dags.krx.krx_co_info.transform_data import transform_krx_co_info_data
from ETL_dags.krx.krx_co_info.load_data_to_s3 import load_krx_co_info_data_to_s3
from ETL_dags.krx.krx_co_info.load_data_to_rds_from_s3 import (
    load_krx_co_info_data_to_rds_from_s3,
)

from ETL_dags.krx.krx_co_info.load_data_to_redshift_from_s3 import (
    load_krx_co_info_data_to_redshift_from_s3,
)

from plugins import slack

task_logger = logging.getLogger("airflow.task")


@task
def extract_krx_co_info():
    task_logger.info("Extract_krx_co_info")
    extract_krx_co_info_data(task_logger)

    return True


@task
def transform_krx_co_info(_):
    task_logger.info("Transform krx_co_info")
    transform_krx_co_info_data(task_logger)

    return True


@task
def load_krx_co_info_to_s3(_):
    task_logger.info("Load_krx_co_info_to_s3")
    load_krx_co_info_data_to_s3(task_logger)

    return True


@task
def load_krx_co_info_to_dw_from_s3(_):
    task_logger.info("Load krx_co_info_to_rds_from_s3")
    load_krx_co_info_data_to_rds_from_s3(task_logger)
    load_krx_co_info_data_to_redshift_from_s3(task_logger)

    return True


with DAG(
    dag_id="krx_co_info_dag17",
    doc_md=doc_md,
    schedule="0 0 * * *",
    start_date=days_ago(1),
    default_args={
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:
    load_krx_co_info_to_dw_from_s3(
        load_krx_co_info_to_s3(transform_krx_co_info(extract_krx_co_info()))
    )
