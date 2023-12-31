doc_md = """
### snp_dag2

#### 전체적인 흐름

1. SNP500에 상장되어 있는 현재 기업의 심볼과, 이름, 산업정보 등을 추출
2. .csv 파일을 s3에 적재
3. s3 -> rds에 적재

#### Dag 특징
1. PythonOperator 사용
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.
3. task간 데이터 이동은 .csv로 로컬에 저장 후 다시 DataFrame으로 불러오는 방식.

##### Domian 특징
1. API의 목적: KRX (KOSPI, KODAQ, KONEX), NASDAQ, NYSE, AMEX, S&P 500 주식 데이터 제공
2. API 유형 및 구조: REDTful API
3. 데이터 형식: JSON이나 라이브러리 자체적으로 DataFrame으로 리턴
4. 업데이트 빈도: 하루 1회
5. 종속성: 없음
6. 제한사항: 대규모 요청의 경우 하루 4회 가능
7. 인증 및 승인: 별도 인증 없음

"""
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import logging
from ETL_dags.snp500.snp_list.extract_data import extract_snp_list_data
from ETL_dags.snp500.snp_list.transform_data import transform_snp_list_data
from ETL_dags.snp500.snp_list.load_data_to_s3 import load_snp_list_data_to_s3
from ETL_dags.snp500.snp_list.load_data_to_rds_from_s3 import (
    load_snp_list_data_to_rds_from_s3,
)
from ETL_dags.snp500.snp_list.load_data_to_redshift_from_s3 import (
    load_snp_list_data_to_redshift_from_s3,
)

from plugins import slack


task_logger = logging.getLogger("airflow.task")  # airflow log에 남기기 위한 사전작업.


@task
def extract_snp_stock_list() -> bool:
    """
    input: None
    output: SNP500 심볼 목록을 ./tmp/snp_stock_list.csv 파일로 저장
    """
    task_logger.info("Extract_snp_stock_list")
    extract_snp_list_data(task_logger)

    return True


@task
def transform_snp_stock_list(_) -> bool:  # 기업 단위로 추출한 주식 데이터 전처리 테스크
    """
    input: snp500의 심볼리스트
    output: 전처리된 snp500 데이터를 ./data/snp_list.csv 파일로 저장하여 전달
    """
    task_logger.info(f"Transform snp_stock_list")
    transform_snp_list_data(task_logger)

    return True


@task
def load_snp_stock_list_to_s3(_) -> bool:  # 기업 단위로 S3에 주식 데이터를 로드하는 테스크
    """
    input: .env 목록
    output: S3에 snp_stock_list.csv 오브젝트로 저장

    """
    task_logger.info(f"Load_snp_stock_list_to_s3")
    load_snp_list_data_to_s3(task_logger)

    return True


@task
def load_snp_stock_list_to_dw_from_s3(_) -> bool:
    """
    input: s3 오브젝트인 snp_stock_list.csv
    output: rds에 raw_data.snp_stock_list table 생성
    """

    task_logger.info(f"Load_snp_stock_list_to_rds_from_s3")
    load_snp_list_data_to_rds_from_s3(task_logger)
    load_snp_list_data_to_redshift_from_s3(task_logger)

    return True


with DAG(
    dag_id="snp_stock_list_dag",  # dag 이름. 코드를 변경하시고 저장하시면 airflow webserver와 동기화 되는데, dag_id가 같으면 dag를 다시 실행할 수 없어, 코드를 변경하시고 dag이름을 임의로 바꾸신후 테스트하시면 편해요. 저는 dag1, dag2, dag3, ... 방식으로 했습니다.
    schedule="0 0 * * *",  # UTC기준 하루단위. 자정에 실행되는 걸로 알고 있습니다.
    start_date=days_ago(1),  # 하루 전으로 설정해서 airflow webserver에서 바로 실행시키도록 했습니다.
    doc_md=doc_md,
    catchup=False,
    tags=["API"],
    default_args={
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:
    load_snp_stock_list_to_dw_from_s3(
        load_snp_stock_list_to_s3(transform_snp_stock_list(extract_snp_stock_list()))
    )
