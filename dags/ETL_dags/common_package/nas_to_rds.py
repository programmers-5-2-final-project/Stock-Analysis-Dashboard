# nas_to_rds.py
import boto3
import psycopg2
from io import StringIO
from dotenv import dotenv_values
import os


def rds_list():
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ
    s3 = boto3.client(  # s3 연결 객체
        "s3",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"],
    )
    # S3 버킷과 파일 이름 설정
    bucket_name = "de-5-2"
    file_name = "nas_list.csv"

    # S3에서 파일 내용 가져오기
    s3_response = s3.get_object(Bucket=bucket_name, Key=file_name)
    csv_content = s3_response["Body"].read().decode("utf-8")

    # StringIO 객체로 변환 (메모리 상에서의 파일 객체처럼 동작)
    f = StringIO(csv_content)
    next(f)  # 헤더 행 건너뛰기

    # PostgreSQL RDS 연결 정보 설정
    db_host = CONFIG["POSTGRES_HOST"]
    db_port = CONFIG["POSTGRES_PORT"]
    db_name = "dev"
    db_user = CONFIG["POSTGRES_USER"]
    db_password = CONFIG["POSTGRES_PASSWORD"]

    # RDS 연결
    connection = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )

    # 새로운 데이터 테이블 생성
    table_name = "nas_list"
    create_table_query = f"""
        DROP TABLE IF EXISTS raw_data.{table_name};
        CREATE TABLE raw_data.{table_name}(
            Symbol VARCHAR(40),
            Name VARCHAR(100),
            Industry VARCHAR(100),
            IndustryCode VARCHAR(40)
        );"""

    cur = connection.cursor()
    cur.execute(create_table_query)
    connection.commit()

    # copy_expert를 사용하여 메모리에서 바로 Postgres로 데이터 업로드
    copy_sql = """
    COPY raw_data.nas_list FROM stdin WITH CSV DELIMITER ','
    """
    cur.copy_expert(sql=copy_sql, file=f)
    connection.commit()

    # 데이터베이스 연결 종료
    cur.close()
    connection.close()


def rds_stock():
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ
    s3 = boto3.client(  # s3 연결 객체
        "s3",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"],
    )
    # S3 버킷과 파일 이름 설정
    bucket_name = "de-5-2"
    file_name = "nas_stock.csv"

    # S3에서 파일 내용 가져오기
    s3_response = s3.get_object(Bucket=bucket_name, Key=file_name)
    csv_content = s3_response["Body"].read().decode("utf-8")

    # StringIO 객체로 변환 (메모리 상에서의 파일 객체처럼 동작)
    f = StringIO(csv_content)
    next(f)  # 헤더 행 건너뛰기

    # PostgreSQL RDS 연결 정보 설정
    db_host = CONFIG["POSTGRES_HOST"]
    db_port = CONFIG["POSTGRES_PORT"]
    db_name = "dev"
    db_user = CONFIG["POSTGRES_USER"]
    db_password = CONFIG["POSTGRES_PASSWORD"]

    # RDS 연결
    connection = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )

    # 새로운 데이터 테이블 생성
    table_name = "nas_stock"
    create_table_query = f"""
        DROP TABLE IF EXISTS raw_data.{table_name};
        CREATE TABLE raw_data.{table_name}(
            Date Date,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            Adj_Close FLOAT,
            Volume FLOAT,
            Symbol VARCHAR(40)
        );"""

    cur = connection.cursor()
    cur.execute(create_table_query)
    connection.commit()

    # copy_expert를 사용하여 메모리에서 바로 Postgres로 데이터 업로드
    copy_sql = """
    COPY raw_data.nas_stock FROM stdin WITH CSV DELIMITER ','
    """
    cur.copy_expert(sql=copy_sql, file=f)
    connection.commit()

    # 데이터베이스 연결 종료
    cur.close()
    connection.close()


rds_list()
rds_stock()
