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

    tmp_krx_stock_csv = "./tmp/krx_stock.csv"
    data_krx_stock_csv = "./data/krx_stock.csv"


class AWS(Enum):
    aws_access_key_id = CONFIG["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = CONFIG["AWS_SECRET_ACCESS_KEY"]
    s3_bucket = "de-5-2"
    region = "ap-northeast-2"


class RDS(Enum):
    rds_user = CONFIG["RDS_USER"]
    rds_password = CONFIG["RDS_PASSWORD"]
    rds_host = CONFIG["RDS_HOST"]
    rds_port = CONFIG["RDS_PORT"]
    rds_dbname = CONFIG["RDS_DB"]


class REDSHIFT(Enum):
    redshift_user = CONFIG["REDSHIFT_USER"]
    redshift_password = CONFIG["REDSHIFT_PASSWORD"]
    redshift_host = CONFIG["REDSHIFT_HOST"]
    redshift_port = CONFIG["REDSHIFT_PORT"]
    redshift_dbname = CONFIG["REDSHIFT_DB"]
