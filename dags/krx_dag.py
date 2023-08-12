from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models.xcom_arg import serialize_xcom_arg
import boto3
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import FinanceDataReader as fdr
from dotenv import dotenv_values
import logging

task_logger = logging.getLogger("airflow.task")

def delete_s3bucket_objects(s3, symbol):
    response = s3.delete_object(Bucket="de-5-2", Key=f"krx_stock_{symbol}.csv")
    if response["DeleteMarker"]:
        task_logger.info(f"Succeed delete krx_stock_{symbol}.csv ")
    else:    
        task_logger.info(f"Failed delete krx_stock_{symbol}.csv ")

@task
def extract_krx_list():
    task_logger.info("Extract_krx_list")
    sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    from api import krx_list
    krx_list_df = krx_list.extract()
    krx_list_df.to_csv("./data/krx_list.csv", index=False, encoding="utf-8-sig")

    return True

@task
def extract_krx_stock(_extract_krx_list, symbol):
    task_logger.info(f"Extract krx_stock_{symbol}")
    raw_df = fdr.DataReader(symbol, "2003")
    raw_df.to_csv(f"./tmp/krx_stock_{symbol}.csv", index=True)
    return True

@task
def transform_krx_stock(_extract_krx_stock, symbol):
    task_logger.info(f"Transform krx_stock_{symbol}")
    raw_df = pd.read_csv(f"./tmp/krx_stock_{symbol}.csv")
    transformed_df = raw_df.drop(columns=["Change"])
    transformed_df = transformed_df.dropna()
    transformed_df.to_csv(f"./data/krx_stock_{symbol}.csv", index=False)
    return True

@task
def load_krx_stock_to_s3(_transform_krx_stock, symbol):
    task_logger.info(f"Load_krx_stock_to_s3_{symbol}")
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"]
    )
    # delete_s3bucket_objects(s3, symbol)
    krx_stock_data = open(f"./data/krx_stock_{symbol}.csv", "rb")
    s3.Bucket("de-5-2").put_object(Key=f"krx_stock_{symbol}.csv", Body=krx_stock_data)
    return True
#
@task
def load_krx_stock_to_rds_from_s3(_load_krx_stock_to_s3, symbol):
    task_logger.info(f"Load krx_stock_to_rds_from_s3_{symbol}")
    # task_logger.info("Installing the aws_s3 extension")
    # engine.execute("CREATE EXTENSION aws_s3 CASCADE;")
    task_logger.info(f"Creating the table krx_stock_{symbol}")
    engine.execute(f"""
                CREATE TABLE krx_stock_{symbol}1(
                id SERIAL PRIMARY KEY,
                Date DATE,
                High VARCHAR(40),
                Low VARCHAR(40),
                Close VARCHAR(40),
                Volume VARCHAR(40)
                );""")
    task_logger.info(f"Importing krx_stock_{symbol}1.csv data from Amazon S3 to RDS for PostgreSQL DB instance")                
    engine.execute(f"""
                SELECT aws_s3.table_import_from_s3(
                'raw_data.krx_stock_{symbol}1', '', '(format csv)',
                ,aws_commons.create_s3_uri('de-5-2', 'krx_stock_{symbol}.csv', 'ap-northeast-2'),
                aws_commons.create_aws_credentials({CONFIG["AWS_ACCESS_KEY_ID"]}, {CONFIG["AWS_SECRET_ACCESS_KEY"]}, '')    
                );""")
    return True

@task
def end(etl):
    task_logger.info("End krx_dag")
    return

with DAG(
    dag_id="krx_dag26",
    schedule = '0 0 * * *',
    start_date = days_ago(1)
) as dag:
    _extract_krx_list = extract_krx_list()
    krx_list = pd.read_csv("./data/krx_list.csv")
    krx_list = krx_list["Code"].tolist()
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ
    
    connection_uri = "postgresql://{}:{}@{}:{}/postgres".format(
        CONFIG["POSTGRES_USER"],
        CONFIG["POSTGRES_PASSWORD"],
        CONFIG["POSTGRES_HOST"],
        CONFIG["POSTGRES_PORT"],
    )
    engine = create_engine(connection_uri, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
    conn = engine.connect()

    etl=[]
    for symbol in krx_list[:2]:
        _extract_krx_stock = extract_krx_stock.override(task_id="extract_krx_stock")(_extract_krx_list, symbol)
        _transform_krx_stock = transform_krx_stock.override(task_id="transform_krx_stock")(_extract_krx_stock, symbol)
        _load_krx_stock_to_s3 = load_krx_stock_to_s3.override(task_id="load_krx_stock_to_s3")(_transform_krx_stock, symbol)
        _load_krx_stock_to_rds_from_s3 = load_krx_stock_to_rds_from_s3.override(task_id="load_krx_stock_to_rds_from_s3")(_load_krx_stock_to_s3, symbol)
        etl.append(_load_krx_stock_to_rds_from_s3)

    conn.close()
    engine.dispose()
    end(etl)