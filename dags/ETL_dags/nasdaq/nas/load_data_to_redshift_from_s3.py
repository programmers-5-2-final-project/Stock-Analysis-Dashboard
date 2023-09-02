from ETL_dags.common.loadToDW import LoadToRedshift
from ETL_dags.common.db import DB
from ETL_dags.krx.constants import REDSHIFT, AWS
import sqlalchemy


def load_nas_list_data_to_redshift_from_s3(task_logger):
    task_logger.info("Creating DB instance")
    db = DB(
        REDSHIFT.redshift_user.value,
        REDSHIFT.redshift_password.value,
        REDSHIFT.redshift_host.value,
        REDSHIFT.redshift_port.value,
        REDSHIFT.redshift_dbname.value,
    )

    task_logger.info("Creating sqlalchemy engine")
    db.create_sqlalchemy_engine()

    task_logger.info("Connecting sqlalchemy engine")
    db.connect_engine()

    task_logger.info("Creating LoadToDW instance")
    load_nas_list = LoadToRedshift(db.conn)

    # 트랜잭션 시작
    trans = db.conn.begin()
    try:
        schema = "raw_data"
        table = "nas_list"

        task_logger.info("Dropping existing raw_data.nas_list")
        load_nas_list.drop_table(schema, table)

        task_logger.info("Creating the table raw_data.nas_list")
        tmp_column_type = {
            "Symbol": "VARCHAR(300)",
            "Name": "VARCHAR(300)",
            "Industry": "VARCHAR(300)",
            "IndustryCode": "VARCHAR(300)",
        }
        # primary_key = "Symbol"
        load_nas_list.create_table(schema, table, tmp_column_type)

        task_logger.info("Importing from s3")
        load_nas_list.table_import_from_s3(
            schema,
            table,
            AWS.s3_bucket.value,
            "nas_list.csv",
        )
        trans.commit()
    except Exception as e:
        trans.rollback()
        raise e

    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()


def load_nas_stock_data_to_redshift_from_s3(task_logger):
    task_logger.info("Creating DB instance")
    db = DB(
        REDSHIFT.redshift_user.value,
        REDSHIFT.redshift_password.value,
        REDSHIFT.redshift_host.value,
        REDSHIFT.redshift_port.value,
        REDSHIFT.redshift_dbname.value,
    )

    task_logger.info("Creating sqlalchemy engine")
    db.create_sqlalchemy_engine()

    task_logger.info("Connecting sqlalchemy engine")
    db.connect_engine()

    task_logger.info("Creating LoadToDW instance")
    load_nas_stock = LoadToRedshift(db.conn)

    # 트랜잭션 시작
    trans = db.conn.begin()
    try:
        schema = "raw_data"
        table = "nas_stock"

        task_logger.info("Dropping existing raw_data.nas_stock")
        load_nas_stock.drop_table(schema, table)

        task_logger.info("Creating the table raw_data.nas_stock")
        tmp_column_type = {
            "Date": "VARCHAR(300)",
            "Open": "VARCHAR(300)",
            "High": "VARCHAR(300)",
            "Low": "VARCHAR(300)",
            "Close": "VARCHAR(300)",
            "Adj_Close": "VARCHAR(300)",
            "Volume": "VARCHAR(300)",
            "Symbol": "VARCHAR(300)",
        }

        primary_key = '"Date", "Symbol"'
        load_nas_stock.create_table(schema, table, tmp_column_type, primary_key)

        task_logger.info("Importing from s3")
        load_nas_stock.table_import_from_s3(
            schema, table, AWS.s3_bucket.value, "nas_stock.csv"
        )

        task_logger.info("Deleting wrong row")
        load_nas_stock.delete_wrong_row(schema, table, "\"Symbol\" like '%Symbol%'")

        # task_logger.info("Altering columns type")
        # real_column_type = {
        #     "Date": "Date",
        #     "Open": "FLOAT",
        #     "High": "FLOAT",
        #     "Low": "FLOAT",
        #     "Close": "FLOAT",
        #     "Adj_Close": "FLOAT",
        #     "Volume": "FLOAT",
        #     "Symbol": "VARCHAR(300)",
        # }
        # load_nas_stock.alter_column_type(schema, table, real_column_type)
        trans.commit()
    except Exception as e:
        trans.rollback()
        raise e
    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()
