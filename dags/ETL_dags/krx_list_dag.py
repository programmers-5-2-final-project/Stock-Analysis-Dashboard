doc_md = """
### krx_list_dag

#### 전체적인 흐름

1. KRX(코스피, 코스닥, 코스넷)에 상장된 기업들 추출
2. 추출한 데이터 처리. Code or Name가 Nan인 행렬은 삭제 처리
3. .csv 파일을 s3에 적재
4. s3 -> rds에 적재

#### Dag 특징
1. PythonOperator만 사용. 다음 스프린트때는 다른 오퍼레이터(ex. S3HookOperator)도 사용할 예정입니다.
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.
3. task간 데이터 이동은 .csv로 로컬에 저장한 후 다시 DataFrame으로 불러오는 방식입니다.

#### Domain 특징
1. http://data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd?baseName=krx.mdc.i18n.component&key=B128.bld
2. cols_map = {'ISU_SRT_CD':'Code', 'ISU_ABBRV':'Name', 
                'TDD_CLSPRC':'Close', 'SECT_TP_NM': 'Dept', 'FLUC_TP_CD':'ChangeCode', 
                'CMPPREVDD_PRC':'Changes', 'FLUC_RT':'ChagesRatio', 'ACC_TRDVOL':'Volume', 
                'ACC_TRDVAL':'Amount', 'TDD_OPNPRC':'Open', 'TDD_HGPRC':'High', 'TDD_LWPRC':'Low',
                'MKTCAP':'Marcap', 'LIST_SHRS':'Stocks', 'MKT_NM':'Market', 'MKT_ID': 'MarketId' }

"""

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import logging

from ETL_dags.krx.krx_list.extract_data import extract_krx_list_data
from ETL_dags.krx.krx_list.transform_data import transform_krx_list_data
from ETL_dags.krx.krx_list.load_data_to_s3 import load_krx_list_data_to_s3
from ETL_dags.krx.krx_list.load_data_to_rds_from_s3 import (
    load_krx_list_data_to_rds_from_s3,
)
from ETL_dags.krx.krx_list.load_data_to_redshift_from_s3 import (
    load_krx_list_data_to_redshift_from_s3,
)
from plugins import slack

task_logger = logging.getLogger("airflow.task")


@task
def extract_krx_list():
    """
    input: None | output: KRX 상장회사(발행회사)목록을 tmp/krx_list.csv에 저장해서 전달
    """
    task_logger.info("Extract_krx_list")

    extract_krx_list_data(task_logger)

    return True


@task
def transform_krx_list(_):
    """
    input: ./tmp/krx_list.csv | output: [Code, Name] 컬럼이 NaN 값이면 행을 삭제한 KRX 상장회사(발행회사)목록을 data/krx_list.csv에 저장해서 전달
    """
    task_logger.info("Transform krx_list")

    transformed_df = transform_krx_list_data(task_logger)

    return True


@task
def load_krx_list_to_s3(_):
    """
    input: ./data/krx_list.csv | output: s3에 krx_list.csv 오브젝트로 저장
    """
    task_logger.info("Load_krx_list_to_s3")

    load_krx_list_data_to_s3(task_logger)

    return True


@task
def load_krx_list_to_dw_from_s3(_):
    """
    input: s3오브젝트인 krx_list.csv | output: rds에 raw_data.krx_list table
    """
    task_logger.info("Load krx_list_to_rds_from_s3")

    load_krx_list_data_to_rds_from_s3(task_logger)
    load_krx_list_data_to_redshift_from_s3(task_logger)

    return True


with DAG(
    dag_id="krx_list_dag48",
    doc_md=doc_md,
    schedule="0 0 * * *",
    start_date=days_ago(1),
    default_args={
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:
    load_krx_list_to_dw_from_s3(
        load_krx_list_to_s3(transform_krx_list(extract_krx_list()))
    )
