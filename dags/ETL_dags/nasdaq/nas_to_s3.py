# nas_to_s3.py
import boto3
from dotenv import dotenv_values
import os


def delete(name):
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ

    s3 = boto3.client(  # s3 연결 객체
        "s3",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"],
    )

    bucket_name = "de-5-2"
    file_key = name

    # 파일 존재 여부 확인
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_key)

    # response의 Contents 리스트에 객체 정보가 들어있다면 해당 파일이 존재함
    if "Contents" in response:
        print(f"File '{file_key}' exists in bucket '{bucket_name}'.")
        s3.delete_object(Bucket="de-5-2", Key="nas_list.csv")
        print("delete success")
    else:
        print(f"File '{file_key}' does not exist in bucket '{bucket_name}'.")


def load():
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ
    nas_list_filepath = "./data/nas_list.csv"
    nas_stock_filepath = "./data/nas_stock.csv"
    s3 = boto3.resource(  # s3 연결 객체
        "s3",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"],
    )
    # 파일 업로드
    delete("nas_list.csv")
    delete("nas_stock.csv")
    with open(nas_list_filepath, "rb") as f:
        s3.Bucket("de-5-2").put_object(Key="nas_list.csv", Body=f)
    with open(nas_stock_filepath, "rb") as f:
        s3.Bucket("de-5-2").put_object(Key="nas_stock.csv", Body=f)


# load()


# delete("nas_list.csv")
