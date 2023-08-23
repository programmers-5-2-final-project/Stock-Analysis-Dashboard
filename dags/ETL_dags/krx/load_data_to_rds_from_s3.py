from ETL_dags.common.loadToDW import LoadToDW
from ETL_dags.krx.constants import RDS, AWS
from sqlalchemy import text


def load_krx_list_data_to_rds_from_s3(task_logger):
    task_logger.info("Creating LoadToDW instance")
    load_krx_to_rds_from_s3 = LoadToDW(
        RDS.rds_user.value,
        RDS.rds_password.value,
        RDS.rds_host.value,
        RDS.rds_port.value,
        RDS.rds_dbname.value,
    )

    task_logger.info("Creating sqlalchemy engine")
    load_krx_to_rds_from_s3.create_sqlalchemy_engine()

    task_logger.info("Connecting sqlalchemy engine")
    load_krx_to_rds_from_s3.connect_engine()

    schema = "raw_data"
    table = "krx_list"

    task_logger.info("Dropping existing raw_data.krx_list")
    load_krx_to_rds_from_s3.drop_table(schema, table)

    task_logger.info("Creating the table raw_data.krx_list")
    tmp_column_type = {
        "Code": "VARCHAR(40)",
        "ISU_CD": "VARCHAR(40)",
        "Name": "VARCHAR(40)",
        "Market": "VARCHAR(40)",
        "Dept": "VARCHAR(40)",
        "Close": "VARCHAR(40)",
        "ChangeCode": "VARCHAR(40)",
        "Changes": "VARCHAR(40)",
        "ChangesRatio": "VARCHAR(40)",
        "Open": "VARCHAR(40)",
        "High": "VARCHAR(40)",
        "Low": "VARCHAR(40)",
        "Volume": "VARCHAR(40)",
        "Amount": "VARCHAR(40)",
        "Marcap": "VARCHAR(40)",
        "Stocks": "VARCHAR(40)",
        "MarketId": "VARCHAR(40)",
    }
    primary_key = "Code"
    load_krx_to_rds_from_s3.create_table(schema, table, tmp_column_type, primary_key)

    task_logger.info("Installing the aws_s3 extension")
    load_krx_to_rds_from_s3.install_aws_s3_extension()  # RDS에 aws_s3 extension 추가. 처음에만 추가하면 돼서 주석처리

    task_logger.info("Importing from s3")
    load_krx_to_rds_from_s3.table_import_from_s3(
        schema,
        table,
        AWS.s3_bucket.value,
        "krx_list.csv",
        "ap-northeast-2",
        AWS.aws_access_key_id.value,
        AWS.aws_secret_access_key.value,
    )

    task_logger.info("Deleting wrong row")
    load_krx_to_rds_from_s3.delete_wrong_row(schema, table, "code like '%Code%'")
    # engine.execute(text("DELETE FROM raw_data.krx_list WHERE code like '%Code%';"))

    task_logger.info("Altering columns type")
    real_column_type = {
        "Code": "VARCHAR(40)",
        "ISU_CD": "VARCHAR(40)",
        "Name": "VARCHAR(40)",
        "Market": "VARCHAR(40)",
        "Dept": "VARCHAR(40)",
        "Close": "INTEGER",
        "ChangeCode": "INTEGER",
        "Changes": "INTEGER",
        "ChangesRatio": "FLOAT",
        "Open": "INTEGER",
        "High": "INTEGER",
        "Low": "INTEGER",
        "Volume": "INTEGER",
        "Amount": "BIGINT",
        "Marcap": "BIGINT",
        "Stocks": "BIGINT",
        "MarketId": "VARCHAR(40)",
    }
    load_krx_to_rds_from_s3.alter_column_type(schema, table, real_column_type)
    # engine.execute(
    #     text(
    #         """
    #             ALTER TABLE raw_data.krx_list
    #                 ALTER COLUMN Close TYPE INTEGER USING Close::INTEGER,
    #                 ALTER COLUMN ChangeCode TYPE INTEGER USING ChangeCode::INTEGER,
    #                 ALTER COLUMN Changes TYPE INTEGER USING Changes::INTEGER,
    #                 ALTER COLUMN ChangesRatio TYPE FLOAT USING ChangesRatio::FLOAT,
    #                 ALTER COLUMN Open TYPE INTEGER USING Open::INTEGER,
    #                 ALTER COLUMN High TYPE INTEGER USING High::INTEGER,
    #                 ALTER COLUMN Low TYPE INTEGER USING Low::INTEGER,
    #                 ALTER COLUMN Volume TYPE INTEGER USING Volume::INTEGER,
    #                 ALTER COLUMN Amount TYPE BIGINT USING Amount::BIGINT,
    #                 ALTER COLUMN Marcap TYPE BIGINT USING Marcap::BIGINT,
    #                 ALTER COLUMN Stocks TYPE BIGINT USING Stocks::BIGINT;
    #             """
    #     )
    # )

    task_logger.info("Closing connection")
    load_krx_to_rds_from_s3.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    load_krx_to_rds_from_s3.dispose_sqlalchemy_engine()
