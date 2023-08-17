from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import BranchPythonOperator

import boto3
import os
import logging
import sys
from dotenv import load_dotenv  # env 파일 사용

# from concurrent.futures import ThreadPoolExecutor
# from botocore.exceptions import NoCredentialsError
# from tempfile import TemporaryDirectory

import pandas as pd
from sqlalchemy import create_engine # 포스트그레스 연결
# import FinanceDataReader as fdr # 주식 데이터 
import quandl # 금, 은 가격
# import pendulum

### 오늘 날짜 생성 -> 추후 next_execution_date 사용할 예정
from datetime import datetime, timedelta
today = datetime.today().strftime('%Y-%m-%d')

task_logger = logging.getLogger("airflow.task") # airflow log에 남기기 위한 사전작업.

# Start 태스크
start_task = DummyOperator(task_id="start")


def decide_branch(**kwargs):
    return ['gold', 'silver', 'cme', 'orb']


### 1. 데이터 가져오기
### 금, 은 가격 가져오기


def get_precious_metal_prices(metal_type='gold', trim_start="2003-01-01", trim_end=today):
    ''' 
    금, 은, 구리, 원유 가격 가져오기
    type : 
        금 : LBMA/GOLD
        은 : LBMA/SILVER
        구리 : CHRIS/CME_HG10
        원유 : OPEC/ORB
        
    {'금' : 'gold',
    '은' : 'silver',
    '구리' : 'cme',
    '원유' : 'orb'}
    '''
    
    ticker = {
            'gold' : 'LBMA/GOLD',
            'silver' : 'LBMA/SILVER',
            'cme' : 'CHRIS/CME_HG10',
            'orb' : 'OPEC/ORB'}
    
    
    file_name=f'{metal_type}_price_{today}'
    
    # .env 파일 로드
    import os
    from dotenv import load_dotenv 
    load_dotenv()

    # 환경 변수에서 액세스 키와 시크릿 키 가져오기
    QUANDL_KEY = os.getenv('QUANDL_KEY')
    
    quandl.ApiConfig.api_key = QUANDL_KEY
    df_metal_price = quandl.get(ticker[metal_type], trim_start=trim_start, trim_end=trim_end)
    
    # 데이터프레임을 CSV 파일로 저장
    csv_filepath = f"/opt/airflow/data/{file_name}.csv"
    df_metal_price.to_csv(csv_filepath, encoding='cp949')
    log_message = "File saved successfully!"
    print(log_message)
    
    return file_name


### 2. s3 연결 
# => 삭제 후 다시 가져오기 

def delete_s3bucket_objects(file_name):
    '''S3 파일 삭제 '''
    # .env 파일 로드
    from dotenv import load_dotenv 
    load_dotenv()

    # 환경 변수에서 액세스 키와 시크릿 키 가져오기
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

    # S3 클라이언트 생성
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    response = s3.delete_object(Bucket="de-5-2", Key=file_name)
    if response["DeleteMarker"]:
        print(f"Succeed delete {file_name}.csv ")
    else:    
        print(f"Failed delete {file_name}.csv ")

# 파일 목록 조회 함수 
def s3_file_list():
    import psycopg2
    from dotenv import load_dotenv 
    load_dotenv()

    # 연결 정보 설정
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    dbname = os.getenv('POSTGRES_DB')
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')  # 실제 비밀번호로 변경하세요

    # 데이터베이스 연결
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

    # 커서 생성
    cur = conn.cursor()
    # 쿼리 실행 (예: 테이블 목록 조회)
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'raw_data';")
    file_list = cur.fetchall()
    task_logger.info(file_list)
    return file_list

@task    
def to_s3(file_name):
    '''
    s3에 적재하는 함수
    '''
    from dotenv import load_dotenv 
    import os
    from datetime import datetime
    today = datetime.today().strftime('%Y-%m-%d')
    
    # .env 파일 로드
    load_dotenv()

    # 환경 변수에서 액세스 키와 시크릿 키 가져오기
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    # aws 추가 설정
    S3_BUCKET = 'de-5-2'
    S3_FILE = file_name
    LOCAL_FILEPATH = f"/opt/airflow/data/{file_name}.csv"
    

    # 로컬 CSV 파일을 S3에 업로드
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    with open(LOCAL_FILEPATH, "rb") as f:
        s3.upload_fileobj(f, S3_BUCKET, f'{S3_FILE}.csv')
        task_logger.info(f"Success_load_s3_{file_name}.csv")
        
    return file_name


### 3. s3 -> RDS



def s3_to_rds(metal_type):
    from io import StringIO
    from sqlalchemy import create_engine
    from dotenv import load_dotenv 
    import boto3
    import psycopg2
    task_logger.info(f"Load s3_to_rds_")
    # .env 파일 로드
    load_dotenv()

    # 환경 변수에서 액세스 키와 시크릿 키 가져오기
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    # S3에서 데이터 로드
    # AWS 설정
    S3_BUCKET = 'de-5-2'
    
    file_name=f'{metal_type}_price_{today}'
    S3_FILE = f'{file_name}.csv'
    

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    response = s3.get_object(Bucket=S3_BUCKET, Key=S3_FILE)
    body = response['Body'].read().decode('utf-8')

    # StringIO 객체로 변환 (메모리 상에서의 파일 객체처럼 동작)
    f = StringIO(body)
    next(f)  # 헤더 행 건너뛰기

    # RDS Postgres 데이터베이스 연결
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    dbname = os.getenv('POSTGRES_DB')
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')  # 실제 비밀번호로 변경하세요

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    cur = conn.cursor()
    
    metal_sql = {'gold' : [f"""DROP TABLE IF EXISTS raw_data.gold;
    CREATE TABLE raw_data.gold (
        date date, 
        USD_AM float, 
        USD_PM float, 
        GBP_AM float, 
        GBP_PM float, 
        EURO_AM float, 
        EURO_PM float,
        type text
    );""", 'raw_data.gold'], 
    'silver' : [f"""DROP TABLE IF EXISTS raw_data.silver;
    CREATE TABLE raw_data.silver (
        date text, 
        USD float, 
        GBP float, 
        EURO float, 
        type text
    );""", 'raw_data.silver'],
    'cme' : 
        [f"""DROP TABLE IF EXISTS raw_data.cme;
    CREATE TABLE raw_data.cme (
        date text, 
        open float,
        high float, 
        low float,
        last float,
        change float,
        volume float,
        Previous_Day_Open_Interest float,
        type text
    );""", 'raw_data.cme'],
    'orb' : 
        [f"""DROP TABLE IF EXISTS raw_data.orb;
    CREATE TABLE raw_data.orb (
        date text, 
        value float, 
        type text
    );""", 'raw_data.orb ']
    }
    cur.execute(metal_sql[metal_type][0])
    
    
    
    # copy_expert를 사용하여 메모리에서 바로 Postgres로 데이터 업로드
    copy_sql = f"""
        COPY {metal_sql[metal_type][1]} FROM stdin WITH CSV DELIMITER ','
    """
    
    cur.copy_expert(sql=copy_sql, file=f)
    conn.commit()
    task_logger.info(f"{metal_sql[metal_type][0]}_to_rds_Success") 

    # 데이터베이스 연결 종료
    cur.close()
    conn.close()
    
    return True





def decide_branch(**kwargs):
    return ['gold', 'silver', 'cme', 'orb']


# BranchPythonOperator를 사용하여 분기 설정
branch_task = BranchPythonOperator(
    task_id="decide_branch",
    python_callable=decide_branch,
)

# Airflow DAG 기본 설정 정의
default_args = {
    'owner': 'airflow', # DAG 소유자
    'depends_on_past': False, # 이전 작업이 성공했을 때만 실행할지 여부
    'retries': 1, # 작업이 실패한 경우 재시도 횟수
    'retry_delay': timedelta(minutes=5), # 작업이 실패한 경우 재시도 간격 = 5분,
    'catchup': True
}


with DAG(
    dag_id="test_parallel_metal_load7", # dag 이름 
    schedule = '0 0 * * *', # UTC기준 하루단위/ 자정에 실행
    default_args=default_args,
    start_date = datetime(2023, 8, 15, hour=0, minute=00) # 시작 날짜, 시간
    
) as dag:
    # DummyOperator로 시작과 끝 지점 생성
    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(task_id='end_task')

    @task
    def get_metal_data(metal):
        file_name = get_precious_metal_prices(metal)
        to_s3(file_name)
        s3_to_rds(metal)

    # for문을 사용하여 병렬 태스크 그룹 생성
    metals = ['gold', 'silver', 'cme', 'orb']
    metal_tasks = []
    
    for metal in metals:
        metal_task = get_metal_data.override(task_id=f"get_metal_data_{metal}")(metal)
        start_task >> metal_task >> end_task
        metal_tasks.append(metal_task)