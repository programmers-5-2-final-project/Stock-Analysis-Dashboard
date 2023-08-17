from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
import boto3
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import FinanceDataReader as fdr
from dotenv import dotenv_values
import logging

'''
✨전체적인 흐름

1. KRX(코스피, 코스닥, 코스넷)에 상장되어 있는 기업의 심볼 가져오기. raw_data.krx_list에서 가져옴
2. 주식데이터 추출
3. 추출한 주식데이터 처리.
4. .csv 파일을 s3에 적재
5. S3->RDS 적재

✨Dag 특징
1. PythonOperator만 사용. 다음 스프린트때는 다른 오퍼레이터(ex. S3HookOperator)도 사용할 예정입니다.
2. task decorator(@task) 사용. task 간 의존성과 순서를 정할때, 좀더 파이썬스러운 방식으로 짤 수 있어 선택했습니다.
3. task간 데이터 이동은 .csv로 로컬에 저장한 후 다시 DataFrame으로 불러오는 방식입니다. 단 krx_list는 xcom방식입니다.
4. 하루단위로 실행. api 자체가 2003.01.02 부터 추출할 수 있어서 start_date를 하루전으로 설정했습니다.

✨Domain 특징
1. https://fchart.stock.naver.com/sise.nhn?timeframe=day&count=6000&requestType=0&symbol=
2. ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'] + 'Change'
'''

task_logger = logging.getLogger("airflow.task") # airflow log에 남기기 위한 사전작업. dag 디버깅하실때, 사용하시면 좋아요!

def delete_s3bucket_objects(s3, symbol): # S3에 저장된 객체를 삭제하는 메서드
    response = s3.delete_object(Bucket="de-5-2", Key=f"krx_stock_{symbol}.csv") 
    if response["DeleteMarker"]:
        task_logger.info(f"Succeed delete krx_stock_{symbol}.csv ")
    else:    
        task_logger.info(f"Failed delete krx_stock_{symbol}.csv ")

@task
def extract_krx_list():  # KRX(코스피, 코스닥, 코스넷)에 상장되어 있는 현재 기업의 심볼을 추출 테스크
    task_logger.info("Extract_krx_list")
    resultproxy = engine.execute(text("SELECT code FROM raw_data.krx_list;"))
    krx_list=[]
    for rowproxy in resultproxy:
        for _, code in rowproxy.items():
            krx_list.append(code)

    return krx_list

@task
def extract_krx_stock(krx_list): # 기업 단위로 주식데이터 추출 테스크
    new_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change','Code']
    df = pd.DataFrame(columns=new_columns)
    df.to_csv("./tmp/krx_stock.csv", index=False) 
    for code in krx_list:
        task_logger.info(f"Extract krx_stock_{code}")
        raw_df = fdr.DataReader(code, "2003")
        raw_df["Code"] = code
        raw_df.to_csv(f"./tmp/krx_stock.csv", mode="a", index=True, header=False) # 다음 테스크로 데이터를 이동시키기 위해 csv 파일로 저장.
    return True

@task
def transform_krx_stock(_): # 기업 단위로 추출한 주식데이터 전처리 테스크
    raw_df = pd.read_csv(f"./tmp/krx_stock.csv") # 저장된 데이터를 다시 DataFrame으로 불러옴 
    transformed_df = raw_df.drop(columns=["Change"]) # Change 컬럼 제거
    transformed_df.dropna(subset=['Date', 'Code'])
    transformed_df.fillna(method = 'ffill', inplace=True)
    new_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Code']
    df = pd.DataFrame(columns=new_columns)
    df.to_csv("./data/krx_stock.csv", index=False)
    transformed_df.to_csv("./data/krx_stock.csv", mode="a", index=False, header=False)
    return True

@task
def load_krx_stock_to_s3(_):   
    task_logger.info("Load_krx_stock_to_s3")
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"]
    ) 
    # delete_s3bucket_objects(s3, symbol) # Full refresh 방식이어서 먼저 S3에 저장된 객체를 삭제. 삭제 권한이 없어 주석처리
    krx_stock_data = open("./data/krx_stock.csv", "rb")
    s3.Bucket("de-5-2").put_object(Key="krx_stock.csv", Body=krx_stock_data)
    return True

@task
def load_krx_stock_to_rds_from_s3(_): # 기업 단위로 S3에 적재한 주식데이터를 RDS(DW)에 COPY 테스크
    task_logger.info("Load krx_stock_to_rds_from_s3")
    # task_logger.info("Installing the aws_s3 extension")
    # engine.execute("CREATE EXTENSION aws_s3 CASCADE;") # RDS에 aws_s3 extension 추가. 처음에만 추가하면 돼서 주석처리
    task_logger.info("Creating the table krx_stock")
    engine.execute(text("""
                DROP TABLE IF EXISTS raw_data.krx_stock;
                CREATE TABLE raw_data.krx_stock(
                Date VARCHAR(40),
                Open VARCHAR(40),
                High VARCHAR(40),
                Low VARCHAR(40),
                Close VARCHAR(40),
                Volume VARCHAR(40),
                Code VARCHAR(40),
                CONSTRAINT pk PRIMARY KEY (Date, Code)   
                );"""))  # RDS에 기업단위로 테이블 생성 쿼리.
    task_logger.info("Importing krx_stock.csv data from Amazon S3 to RDS for PostgreSQL DB instance")                
    engine.execute(text(f"""
                SELECT aws_s3.table_import_from_s3(
                'raw_data.krx_stock', '', '(format csv)',
                aws_commons.create_s3_uri('de-5-2', 'krx_stock.csv', 'ap-northeast-2'),
                aws_commons.create_aws_credentials('{CONFIG["AWS_ACCESS_KEY_ID"]}', '{CONFIG["AWS_SECRET_ACCESS_KEY"]}', '')    
                );""")) # S3에서 RDS로 복사하는 쿼리. 자세한 정보는 https://docs.aws.amazon.com/ko_kr/AmazonRDS/latest/UserGuide/USER_PostgreSQL.S3Import.html#aws_s3.table_import_from_s3
    task_logger.info("Converting column types")
    engine.execute(text("DELETE FROM raw_data.krx_stock WHERE code like '%Code%';")) # 첫 행이 header여서 지워주는 쿼리
    engine.execute(text("""
                ALTER TABLE raw_data.krx_stock
                    ALTER COLUMN Date TYPE TIMESTAMP USING Date::TIMESTAMP,
                    ALTER COLUMN Open TYPE INTEGER USING Open::INTEGER,
                    ALTER COLUMN High TYPE INTEGER USING High::INTEGER,
                    ALTER COLUMN Low TYPE INTEGER USING Low::INTEGER,
                    ALTER COLUMN Close TYPE INTEGER USING Close::INTEGER,    
                    ALTER COLUMN Volume TYPE INTEGER USING Volume::INTEGER;
                """))
    return True

with DAG(
    dag_id="krx_stock_dag11", # dag 이름. 코드를 변경하시고 저장하시면 airflow webserver와 동기화 되는데, dag_id가 같으면 dag를 다시 실행할 수 없어, 코드를 변경하시고 dag이름을 임의로 바꾸신후 테스트하시면 편해요. 저는 dag1, dag2, dag3, ... 방식으로 했습니다.
    schedule = '0 0 * * *', # UTC기준 하루단위. 자정에 실행되는 걸로 알고 있습니다.
    start_date = days_ago(1) # 하루 전으로 설정해서 airflow webserver에서 바로 실행시키도록 했습니다.
) as dag:
    CONFIG = dotenv_values(".env") # .env 파일에 숨겨진 값(AWS ACCESS KEY)을 사용하기 위함. 
    if not CONFIG:
        CONFIG = os.environ

    #RDS에 접속
    connection_uri = "postgresql://{}:{}@{}:{}/{}".format(
        CONFIG["POSTGRES_USER"],
        CONFIG["POSTGRES_PASSWORD"],
        CONFIG["POSTGRES_HOST"],
        CONFIG["POSTGRES_PORT"],
        CONFIG["POSTGRES_DB"]
    ) 
    engine = create_engine(connection_uri, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
    conn = engine.connect()

    krx_list = extract_krx_list() # KRX(코스피, 코스닥, 코스넷)에 상장되어 있는 현재 기업의 심볼을 추출 테스크 실행
    load_krx_stock_to_rds_from_s3(load_krx_stock_to_s3(transform_krx_stock((extract_krx_stock(krx_list)))))

    # RDS close
    conn.close()
    engine.dispose()