from enum import Enum
from dotenv import dotenv_values
import os

CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ


class FilePath(Enum):
    data_gold_price_csv = "./data/gold_price.csv"
    data_silver_price_csv = "./data/silver_price.csv"
    data_cme_price_csv = "./data/cme_price.csv"
    data_orb_price_csv = "./data/orb_price.csv"


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


class Ticker(Enum):
    gold = "LBMA/GOLD"
    silver = "LBMA/SILVER"
    cme = "CHRIS/CME_HG10"
    orb = "OPEC/ORB"


class API_KEY(Enum):
    QUANDL_KEY = CONFIG["QUANDL_KEY"]
