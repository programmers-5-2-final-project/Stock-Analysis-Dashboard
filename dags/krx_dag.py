from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.models import Variable
import boto3
import os
import sys
import pandas as pd
import pendulum
from sqlalchemy import create_engine
import FinanceDataReader as fdr
from dotenv import dotenv_values

def delete_s3bucket_objects(s3, symbol):
    response = s3.delete_object(Bucket="de-5-2", Key=f"krx_stock_{symbol}.csv")
    if response["DeleteMarker"]:
        print(f"Succeed delete krx_stock_{symbol}.csv ")
    else:    
        print(f"Failed delete krx_stock_{symbol}.csv ")

@task
def extract_krx_list(**context):
    sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    from api import krx_list
    krx_list_df = krx_list.extract()
    print("Extract_krx_list")
    krx_list_df.to_csv("/data/krx_list.csv", index=False, encoding="utf-8-sig")
    krx_list = krx_list_df["Code"].tolist()
    Variable.update("krx_list", krx_list)

    return True

@task
def extract_krx_stock(symbol):
    raw_df = fdr.DataReader(symbol, "2003")
    raw_df.to_csv(f"./tmp/krx_stock_{symbol}.csv", index=True)
    return True

@task
def transform_krx_stock(_extract_krx_stock, symbol):
    raw_df = pd.read_csv(f"/tmp/krx_stock_{symbol}.csv")
    transformed_df = raw_df.drop(columns=["Change"])
    transformed_df = transformed_df.dropna()
    transformed_df.to_csv(f"/data/krx_stock_{symbol}.csv", index=False)
    return True

@task
def load_krx_stock_to_s3(_transform_krx_stock, symbol):
    s3 = boto3.resource("s3")
    # delete_s3bucket_objects(s3, symbol)
    krx_stock_data = open(f"/data/krx_stock_{symbol}.csv", "rb")
    s3.Bucket("de-5-2").put_object(Key=f"krx_stock_{symbol}.csv", Body=krx_stock_data)
    return True

@task
def load_krx_stock_to_rds_from_s3(_load_krx_stock_to_s3, symbol):
    print("Installing the aws_s3 extension")
    engine.execute("CREATE EXTENSION aws_s3 CASCADE;")
    print(f"Creating the table krx_stock_{symbol}")
    engine.execute(f"""
                CREATE TABLE raw_data.krx_stock_{symbol}(
                id SERIAL PRIMARY KEY,
                Date DATE,
                High VARCHAR(40),
                Low VARCHAR(40),
                Close VARCHAR(40),
                Volume VARCHAR(40)
                """)
    print(f"Importing krx_stock_{symbol}.csv data from Amazon S3 to RDS for PostgreSQL DB instance")                
    engine.execute(f"""
                SELECT aws_s3.table_import_from_s3(
                'raw_data.krx_stock_{symbol}', '', '(format csv)',
                ,aws_commons.create_s3_uri('de-5-2', 'krx_stock_{symbol}.csv', 'ap-northeast-2')
                aws_commons.create_aws_credentials({CONFIG["AWS_ACCESS_KEY_ID"]}, {CONFIG["AWS_SECRET_ACCESS_KEY"]}, '')    
                """)
    return True

@task
def end(etl):
    print("End krx_dag")
    return

with DAG(
    dag_id="krx_dag",
    schedule = '0 0 * * *',
    start_date = pendulum.datetime(2003,1,1)
) as dag:
    extract_krx_list()
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ
    try:
        os.mkdir("tmp")
        os.chmod("tmp", 0o0777)
    except:
        print("Already tmp dir exists")
    
    print(f'Connecting to DB, DB name is {CONFIG["POSTGRES_DB"]}')
    connection_uri = "postgresql://{}:{}@{}:{}/postgres".format(
        CONFIG["POSTGRES_USER"],
        CONFIG["POSTGRES_PASSWORD"],
        CONFIG["POSTGRES_HOST"],
        CONFIG["POSTGRES_PORT"],
    )
    engine = create_engine(connection_uri, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
    conn = engine.connect()

    etl=[]

    krx_list = Variable.get("krx_list")
    for symbol in krx_list[:2]:
        _extract_krx_stock = extract_krx_stock.override(task_id="extract_krx_stock_"+symbol)(symbol)
        _transform_krx_stock = transform_krx_stock.override(task_id="transform_krx_stock_"+symbol)(_extract_krx_stock, symbol)
        _load_krx_stock_to_s3 = load_krx_stock_to_s3.override(task_id="load_krx_stock_to_s3_"+symbol)(_transform_krx_stock, symbol)
        _load_krx_stock_to_rds_from_s3 = load_krx_stock_to_rds_from_s3.override(task_id="load_krx_stock_to_rds_from_s3_"+symbol)(_load_krx_stock_to_s3, symbol)
        etl.append(_load_krx_stock_to_rds_from_s3)

    conn.close()
    engine.dispose()
    os.remove("tmp")
    end(etl)