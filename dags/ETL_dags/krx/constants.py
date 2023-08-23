from enum import Enum
from dotenv import dotenv_values
import os

CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ


class FilePath(Enum):
    tmp_krx_list_csv = "./tmp/krx_list.csv"
    data_krx_list_csv = "./data/krx_list.csv"

    tmp_krx_co_info_csv = "./tmp/krx_co_info.csv"
    data_krx_co_info_csv = "./data/krx_co_info.csv"


class AWS(Enum):
    aws_access_key_id = CONFIG["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = CONFIG["AWS_SECRET_ACCESS_KEY"]
    s3_bucket = "de-5-2"
    region = "ap-northeast-2"


class RDS(Enum):
    rds_user = CONFIG["POSTGRES_USER"]
    rds_password = CONFIG["POSTGRES_PASSWORD"]
    rds_host = CONFIG["POSTGRES_HOST"]
    rds_port = CONFIG["POSTGRES_PORT"]
    rds_dbname = CONFIG["POSTGRES_DB"]
