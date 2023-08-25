from enum import Enum
from dotenv import dotenv_values
import os

CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ


class FilePath(Enum):
    data_nas_list_csv = "./data/nas_list.csv"
    data_nas_stock_csv = "./data/nas_stock.csv"
    data_nas_co_info_csv = "./data/nas_co_info.csv"
    data_nas_co_info_t_csv = "./data/nas_com_info_t.csv"


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
