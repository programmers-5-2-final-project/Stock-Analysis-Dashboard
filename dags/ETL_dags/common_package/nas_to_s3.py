# nas_to_s3.py
import boto3
from dotenv import dotenv_values
import os


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

    # for i in ['nas_list', 'nas_stock']:
    # 파일이 이미 존재하는지 확인
    # response = s3.list_objects(Bucket="de-5-2", Prefix=i)
    # file_exists = len(response.get('Contents', [])) > 0

    # 파일 업로드
    # if file_exists:
    #     s3.delete_object(Bucket="de-5-2", Key=i)
    with open(nas_list_filepath, "rb") as f:
        s3.Bucket("de-5-2").put_object(Key=f"nas_list.csv", Body=f)
    with open(nas_stock_filepath, "rb") as f:
        s3.Bucket("de-5-2").put_object(Key=f"nas_stock.csv", Body=f)
