from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

import logging
from datetime import datetime
from datetime import datetime, timedelta
from ETL_dags.raw_material.extract_data import extract_raw_material_price_data
from ETL_dags.raw_material.load_data_to_s3 import load_raw_material_price_data_to_s3
from ETL_dags.raw_material.load_data_to_rds_from_s3 import (
    load_raw_material_price_data_to_rds_from_s3,
)
from plugins import slack

from ETL_dags.raw_material.load_data_to_redshift_from_s3 import (
    load_raw_material_price_data_to_redshift_from_s3,
)

task_logger = logging.getLogger("airflow.task")


@task
def set_start_end_date():
    task_logger.info("Setting start_date and end_date")
    dt = datetime.now()
    end_date = dt.strftime("%Y-%m-%d")
    dt = dt.replace(year=dt.year - 20)
    start_date = dt.strftime("%Y-%m-%d")
    return [start_date, end_date]


@task
def extract_raw_material_price(date, raw_material):
    task_logger.info("Extract raw material price")
    start_date, end_date = date[0], date[1]
    extract_raw_material_price_data(task_logger, raw_material, start_date, end_date)

    return raw_material


@task
def load_raw_material_price_to_s3(raw_material):
    task_logger.info(f"load_{raw_material}_price_to_s3")
    load_raw_material_price_data_to_s3(task_logger, raw_material)

    return raw_material


@task
def load_raw_material_price_to_dw_from_s3(raw_material):
    task_logger.info(f"load_{raw_material}_price_to_dw_from_s3")
    load_raw_material_price_data_to_rds_from_s3(task_logger, raw_material)
    load_raw_material_price_data_to_redshift_from_s3(task_logger, raw_material)

    return True


@task
def end(_):
    task_logger.info("end")
    return


default_args = {
    "owner": "airflow",  # DAG 소유자
    "depends_on_past": False,  # 이전 작업이 성공했을 때만 실행할지 여부
    "retries": 1,  # 작업이 실패한 경우 재시도 횟수
    "retry_delay": timedelta(minutes=5),  # 작업이 실패한 경우 재시도 간격 = 5분,
    "catchup": True,
    "on_failure_callback": slack.on_failure_callback,
}


with DAG(
    dag_id="raw_materials_dag26",
    schedule="0 0 * * *",
    start_date=days_ago(1),
    default_args=default_args,
) as dag:
    date = set_start_end_date()

    raw_materials = ["gold", "silver", "cme", "orb"]
    etl = []
    for raw_material in raw_materials:
        _raw_material = extract_raw_material_price.override(
            task_id=f"extract_{raw_material}_price"
        )(date, raw_material)
        _raw_material = load_raw_material_price_to_s3.override(
            task_id=f"load_{raw_material}_price_to_s3"
        )(_raw_material)
        result = load_raw_material_price_to_dw_from_s3.override(
            task_id=f"load_{raw_material}_price_to_dw_from_s3"
        )(_raw_material)
        etl.append(result)
    end(etl)
