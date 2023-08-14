from datetime import datetime
from airflow import DAG
from airflow.decorators import task, dag
import boto3
import psycopg2
from io import StringIO

@task
def s3_to_rds():
    # S3에서 데이터 로드
    # AWS 설정
    AWS_ACCESS_KEY = 'AKIA4RRVVY55YXCM2MPC'
    AWS_SECRET_KEY = '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'
    S3_BUCKET = 'de-5-2'
    S3_FILE = 'test.csv'
    # LOCAL_FILE = 'name_gender.csv'

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    
    response = s3.get_object(Bucket=S3_BUCKET, Key=S3_FILE)
    body = response['Body'].read().decode('utf-8')

    # StringIO 객체로 변환 (메모리 상에서의 파일 객체처럼 동작)
    f = StringIO(body)
    next(f)  # 헤더 행 건너뛰기

    # RDS Postgres 데이터베이스 연결
    DATABASE_USERNAME = 'postgres'
    DATABASE_PASSWORD = 'kdt_de_52!'
    DATABASE_HOSTNAME = "de-5-2-db.ch4xfyi6stod.ap-northeast-2.rds.amazonaws.com"
    DATABASE_PORT = '5432'
    DATABASE_NAME = 'dev'

    conn = psycopg2.connect(
        dbname=DATABASE_NAME,
        user=DATABASE_USERNAME,
        password=DATABASE_PASSWORD,
        host=DATABASE_HOSTNAME,
        port=DATABASE_PORT
    )
    cur = conn.cursor()

    # copy_expert를 사용하여 메모리에서 바로 Postgres로 데이터 업로드
    copy_sql = """
        COPY raw_data.test FROM stdin WITH CSV DELIMITER ','
    """
    cur.copy_expert(sql=copy_sql, file=f)
    conn.commit()

    # 데이터베이스 연결 종료
    cur.close()
    conn.close()

with DAG(
    dag_id='SNP500_S3_To_Rds',
    start_date=datetime(2023, 8, 10),
    catchup=False,
    tags=['API'],
    schedule_interval='0 10 * * *'
) as dag:

    s3_to_rds()