from ETL_dags.common.extract import Extract
from ETL_dags.common.db import DB
from ETL_dags.krx.constants import FilePath, RDS
from ETL_dags.common.csv import df_to_csv
from sqlalchemy import text
import billiard as mp
import FinanceDataReader as fdr
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

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

    trans = db.conn.begin()
    try:
        task_logger.info("Extracting krx stock data")
        resultproxy = db.conn.execute(text("SELECT \"Code\" FROM raw_data.krx_list;"))
        krx_list = []
        for rowproxy in resultproxy:
            for _, code in rowproxy.items():
                krx_list.append(code)
        trans.commit()
    except Exception as e:
        trans.rollback()
        raise e

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


def extract_all_krx_stock_data(krx_list):
    new_columns = ["Date", "Open", "High", "Low", "Close", "Volume", "Change", "Code"]
    df = pd.DataFrame(columns=new_columns)
    df.to_csv(FilePath.tmp_krx_stock_csv.value, index=False)
    with ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(extract_krx_stock_data, krx_list)

    # cpu_count = mp.cpu_count() - 2
    # with mp.Pool(cpu_count) as pool:
    #     pool.map(extract_krx_stock_data, krx_list)
