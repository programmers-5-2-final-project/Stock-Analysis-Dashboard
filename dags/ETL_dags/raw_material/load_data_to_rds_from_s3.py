from ETL_dags.common.loadToDW import LoadToDW
from ETL_dags.common.db import DB
from ETL_dags.raw_material.constants import RDS, AWS
import sqlalchemy


def load_raw_material_price_data_to_rds_from_s3(task_logger, raw_material):
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
    load_raw_material_to_rds_from_s3 = LoadToDW(db.engine)

    schema = "raw_data"
    table = f"{raw_material}_price"

    task_logger.info(f"Dropping existing {schema}.{table}")
    load_raw_material_to_rds_from_s3.drop_table(schema, table)

    task_logger.info(f"Creating the table {schema}.{table}")
    tmp_gold_column_type = {
        "date": "VARCHAR(40)",
        "USD_AM": "VARCHAR(40)",
        "USD_PM": "VARCHAR(40)",
        "GBP_AM": "VARCHAR(40)",
        "GBP_PM": "VARCHAR(40)",
        "EURO_AM": "VARCHAR(40)",
        "EURO_PM": "VARCHAR(40)",
    }
    real_gold_column_type = {
        "date": "DATE",
        "USD_AM": "FLOAT",
        "USD_PM": "FLOAT",
        "GBP_AM": "FLOAT",
        "GBP_PM": "FLOAT",
        "EURO_AM": "FLOAT",
        "EURO_PM": "FLOAT",
    }
    tmp_silver_column_type = {
        "date": "VARCHAR(40)",
        "USD": "VARCHAR(40)",
        "GBP": "VARCHAR(40)",
        "EURO": "VARCHAR(40)",
    }
    real_silver_column_type = {
        "date": "DATE",
        "USD": "FLOAT",
        "GBP": "FLOAT",
        "EURO": "FLOAT",
    }
    tmp_cme_column_type = {
        "date": "VARCHAR(40)",
        "open": "VARCHAR(40)",
        "high": "VARCHAR(40)",
        "low": "VARCHAR(40)",
        "last": "VARCHAR(40)",
        "change": "VARCHAR(40)",
        "settle": "VARCHAR(40)",
        "volume": "VARCHAR(40)",
        "Previous_Day_Open_Interest": "VARCHAR(40)",
    }
    real_cme_column_type = {
        "date": "DATE",
        "open": "FLOAT",
        "high": "FLOAT",
        "low": "FLOAT",
        "last": "FLOAT",
        "change": "FLOAT",
        "settle": "FLOAT",
        "volume": "FLOAT",
        "Previous_Day_Open_Interest": "FLOAT",
    }
    tmp_orb_column_type = {"date": "VARCHAR(40)", "value": "VARCHAR(40)"}
    real_orb_column_type = {"date": "DATE", "value": "FLOAT"}

    if raw_material == "gold":
        tmp_column_type = tmp_gold_column_type
        real_column_type = real_gold_column_type
    elif raw_material == "silver":
        tmp_column_type = tmp_silver_column_type
        real_column_type = real_silver_column_type
    elif raw_material == "cme":
        tmp_column_type = tmp_cme_column_type
        real_column_type = real_cme_column_type
    elif raw_material == "orb":
        tmp_column_type = tmp_orb_column_type
        real_column_type = real_orb_column_type

    primary_key = "date"
    load_raw_material_to_rds_from_s3.create_table(
        schema, table, tmp_column_type, primary_key
    )

    try:
        task_logger.info("Installing the aws_s3 extension")
        load_raw_material_to_rds_from_s3.install_aws_s3_extension()
    except sqlalchemy.exc.ProgrammingError:
        task_logger.info("aws_s3 extension already exists")

    task_logger.info("Importing from s3")
    load_raw_material_to_rds_from_s3.table_import_from_s3(
        schema,
        table,
        AWS.s3_bucket.value,
        f"{raw_material}_price.csv",
        AWS.region.value,
        AWS.aws_access_key_id.value,
        AWS.aws_secret_access_key.value,
    )

    task_logger.info("Deleting wrong row")
    load_raw_material_to_rds_from_s3.delete_wrong_row(
        schema, table, "date like '%date%'"
    )

    task_logger.info("Altering columns type")
    load_raw_material_to_rds_from_s3.alter_column_type(schema, table, real_column_type)

    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()
