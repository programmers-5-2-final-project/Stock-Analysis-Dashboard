from ETL_dags.common.loadToDW import LoadToRDS
from ETL_dags.common.db import DB
from ETL_dags.krx.constants import RDS, AWS
import sqlalchemy


def load_krx_list_data_to_rds_from_s3(task_logger):
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

    load_krx_to_rds_from_s3 = LoadToRDS(db.conn)

    try:
        task_logger.info("Installing the aws_s3 extension")
        load_krx_to_rds_from_s3.install_aws_s3_extension()
    except sqlalchemy.exc.ProgrammingError:
        task_logger.info("aws_s3 extension already exists")

    # 트랜잭션 시작
    trans = db.conn.begin()
    try:
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
        primary_key = '"Code"'
        load_krx_to_rds_from_s3.create_table(
            schema, table, tmp_column_type, primary_key
        )

        task_logger.info("Importing from s3")
        load_krx_to_rds_from_s3.table_import_from_s3(
            schema,
            table,
            AWS.s3_bucket.value,
            "krx_list.csv",
            AWS.region.value,
            AWS.aws_access_key_id.value,
            AWS.aws_secret_access_key.value,
        )

        task_logger.info("Deleting wrong row")
        load_krx_to_rds_from_s3.delete_wrong_row(schema, table, '"Code" like \'%Code%\'')

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
        trans.commit()
    except Exception as e:
        trans.rollback()
        raise e
    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()
