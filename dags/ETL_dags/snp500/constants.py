from enum import Enum
from dotenv import dotenv_values
import os

CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ


class FilePath(Enum):
    tmp_snp_list_csv = "./tmp/snp_stock_list.csv"
    data_snp_list_csv = "./data/snp_stock_list.csv"
    tmp_snp_stock_csv = "./tmp/snp_stock.csv"
    data_snp_stock_csv = "./data/snp_stock.csv"


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
