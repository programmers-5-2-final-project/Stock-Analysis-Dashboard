from ETL_dags.common.extract import Extract
from ETL_dags.common.db import DB
from ETL_dags.krx.constants import FilePath, RDS
from ETL_dags.common.csv import df_to_csv
from sqlalchemy import text
import billiard as mp
import FinanceDataReader as fdr
import pandas as pd


def extract_krx_list_data_from_rds(task_logger):
    task_logger.info("Creating DB instance")
    db = DB(
        RDS.rds_user.value,
        RDS.rds_password.value,
        RDS.rds_host.value,
        RDS.rds_port.value,
        RDS.rds_dbname.value,
    )

    task_logger.info("Creating sqlalchemy engine")
    db.create_sqlalchemy_engine()

    task_logger.info("Connecting sqlalchemy engine")
    db.connect_engine()

    task_logger.info("Extracting krx stock data")
    resultproxy = db.engine.execute(text("SELECT code FROM raw_data.krx_list;"))
    krx_list = []
    for rowproxy in resultproxy:
        for _, code in rowproxy.items():
            krx_list.append(code)

    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()

    return krx_list


def extract_krx_stock_data(code):
    try:
        print(f"Extract krx_stock_{code}")
        raw_df = fdr.DataReader(code, "2003")
        raw_df["Code"] = code
        raw_df.to_csv(
            FilePath.tmp_krx_stock_csv.value, mode="a", index=True, header=False
        )
    except Exception as e:
        print(e)
