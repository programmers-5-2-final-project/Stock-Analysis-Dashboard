# nas&snp_co_info_dag.py
from airflow import DAG
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from dotenv import dotenv_values
import logging
import FinanceDataReader as fdr
import logging
from ETL_dags.nasdaq import nas_co_info_, snp_co_info_

task_logger = logging.getLogger("airflow.task")
from plugins import slack


@task
def extract_nas_co_info():
    task_logger.info("Extract_nas_stock")
    nas_co_info_.extract()
    return


@task
def load_nas_co_info_to_s3():
    task_logger.info(f"Load_nas_com_info_to_s3")
    nas_co_info_.load()
    return True


@task
def load_nas_co_info_to_rds_from_s3():
    task_logger.info(f"Load_nas_com_info_to_rds_from_s3")
    nas_co_info_.rds()
    return True


@task
def extract_snp_co_info():
    task_logger.info("Extract_snp_stock")
    snp_co_info_.extract()
    return


@task
def load_snp_co_info_to_s3():
    task_logger.info(f"Load_snp_co_info_to_s3")
    snp_co_info_.load()
    return True


@task
def load_snp_co_info_to_rds_from_s3():
    task_logger.info(f"Load_snp_co_info_to_rds_from_s3")
    snp_co_info_.rds()
    return True


with DAG(
    dag_id="nas_snp_co_info_dag",  # dag 이름. 코드를 변경하시고 저장하시면 airflow webserver와 동기화 되는데, dag_id가 같으면 dag를 다시 실행할 수 없어, 코드를 변경하시고 dag이름을 임의로 바꾸신후 테스트하시면 편해요. 저는 dag1, dag2, dag3, ... 방식으로 했습니다.
    schedule="0 0 * * *",  # UTC기준 하루단위. 자정에 실행되는 걸로 알고 있습니다.
    start_date=days_ago(1),  # 하루 전으로 설정해서 airflow webserver에서 바로 실행시키도록 했습니다.
    catchup=False,  # 과거의 task를 실행할지 여부. False로 설정하면, 과거의 task는 실행되지 않습니다.
    default_args={
        "on_failure_callback": slack.on_failure_callback,
    },
) as dag:
    (
        extract_nas_co_info()
        >> load_nas_co_info_to_s3()
        >> load_nas_co_info_to_rds_from_s3()
        >> extract_snp_co_info()
        >> load_snp_co_info_to_s3()
        >> load_snp_co_info_to_rds_from_s3()
    )  # dag의 task를 순서대로 연결해줍니다. >> 를 사용하시면 됩니다.
