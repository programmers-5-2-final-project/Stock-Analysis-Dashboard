from ETL_dags.common.loadToDW import LoadToDW
from ETL_dags.common.db import DB
from ETL_dags.krx.constants import RDS, AWS
import sqlalchemy


def load_nas_list_data_to_rds_from_s3(task_logger):
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

    task_logger.info("Creating LoadToDW instance")
    load_nas_list = LoadToDW(db.engine)

    schema = "raw_data"
    table = "nas_list"

    task_logger.info("Dropping existing raw_data.nas_list")
    load_nas_list.drop_table(schema, table)

    task_logger.info("Creating the table raw_data.nas_list")
    tmp_column_type = {
        "Symbol": "VARCHAR(40)",
        "Name": "VARCHAR(100)",
        "Industry": "VARCHAR(100)",
        "IndustryCode": "VARCHAR(40)",
    }
    # primary_key = "Symbol"
    load_nas_list.create_table(schema, table, tmp_column_type)

    try:
        task_logger.info("Installing the aws_s3 extension")
        load_nas_list.install_aws_s3_extension()
    except sqlalchemy.exc.ProgrammingError:
        task_logger.info("aws_s3 extension already exists")

    task_logger.info("Importing from s3")
    load_nas_list.table_import_from_s3(
        schema,
        table,
        AWS.s3_bucket.value,
        "nas_list.csv",
        AWS.region.value,
        AWS.aws_access_key_id.value,
        AWS.aws_secret_access_key.value,
    )

    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()


def load_nas_stock_data_to_rds_from_s3(task_logger):
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

    task_logger.info("Creating LoadToDW instance")
    load_nas_stock = LoadToDW(db.engine)

    schema = "raw_data"
    table = "nas_stock"

    task_logger.info("Dropping existing raw_data.nas_stock")
    load_nas_stock.drop_table(schema, table)

    task_logger.info("Creating the table raw_data.nas_stock")
    tmp_column_type = {
        "Date": "VARCHAR(40)",
        "Open": "VARCHAR(40)",
        "High": "VARCHAR(40)",
        "Low": "VARCHAR(40)",
        "Close": "VARCHAR(40)",
        "Adj_Close": "VARCHAR(40)",
        "Volume": "VARCHAR(40)",
        "Symbol": "VARCHAR(40)",
    }

    primary_key = "Date, Symbol"
    load_nas_stock.create_table(schema, table, tmp_column_type, primary_key)

    try:
        task_logger.info("Installing the aws_s3 extension")
        load_nas_stock.install_aws_s3_extension()
    except sqlalchemy.exc.ProgrammingError:
        task_logger.info("aws_s3 extension already exists")

    task_logger.info("Importing from s3")
    load_nas_stock.table_import_from_s3(
        schema,
        table,
        AWS.s3_bucket.value,
        "nas_stock.csv",
        AWS.region.value,
        AWS.aws_access_key_id.value,
        AWS.aws_secret_access_key.value,
    )

    task_logger.info("Deleting wrong row")
    load_nas_stock.delete_wrong_row(schema, table, "symbol like '%Symbol%'")

    task_logger.info("Altering columns type")
    real_column_type = {
        "Date": "Date",
        "Open": "FLOAT",
        "High": "FLOAT",
        "Low": "FLOAT",
        "Close": "FLOAT",
        "Adj_Close": "FLOAT",
        "Volume": "FLOAT",
        "Symbol": "VARCHAR(40)",
    }
    load_nas_stock.alter_column_type(schema, table, real_column_type)

    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()
