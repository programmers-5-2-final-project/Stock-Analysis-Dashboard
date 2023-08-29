from ETL_dags.common.loadToDW import LoadToRedshift
from ETL_dags.common.db import DB
from ETL_dags.krx.constants import RDS, AWS, REDSHIFT
import sqlalchemy


def load_krx_list_data_to_redshift_from_s3(task_logger):
    task_logger.info("Creating DB instance")
    db = DB(
        REDSHIFT.rds_user.value,
        REDSHIFT.rds_password.value,
        REDSHIFT.rds_host.value,
        REDSHIFT.rds_port.value,
        REDSHIFT.rds_dbname.value,
    )

    task_logger.info("Creating sqlalchemy engine")
    db.create_sqlalchemy_engine()

    task_logger.info("Connecting sqlalchemy engine")
    db.connect_engine()

    task_logger.info("Creating LoadToRedshift instance")
    load_krx_to_redshift_from_s3 = LoadToRedshift(db.engine)

    # 트랜잭션 시작
    trans = db.conn.begin()
    try:
        schema = "raw_data"
        table = "krx_list"

        task_logger.info("Dropping existing raw_data.krx_list")
        load_krx_to_redshift_from_s3.drop_table(schema, table)

        task_logger.info("Creating the table raw_data.krx_list")
        tmp_column_type = {
            "Code": "VARCHAR(300)",
            "ISU_CD": "VARCHAR(300)",
            "Name": "VARCHAR(300)",
            "Market": "VARCHAR(300)",
            "Dept": "VARCHAR(300)",
            "Close": "VARCHAR(300)",
            "ChangeCode": "VARCHAR(300)",
            "Changes": "VARCHAR(300)",
            "ChangesRatio": "VARCHAR(300)",
            "Open": "VARCHAR(300)",
            "High": "VARCHAR(300)",
            "Low": "VARCHAR(300)",
            "Volume": "VARCHAR(300)",
            "Amount": "VARCHAR(300)",
            "Marcap": "VARCHAR(300)",
            "Stocks": "VARCHAR(300)",
            "MarketId": "VARCHAR(300)",
        }
        primary_key = "Code"
        load_krx_to_redshift_from_s3.create_table(
            schema, table, tmp_column_type, primary_key
        )

        task_logger.info("Importing from s3")
        load_krx_to_redshift_from_s3.table_import_from_s3(
            schema, table, AWS.s3_bucket.value, "krx_list.csv"
        )

        # task_logger.info("Deleting wrong row")
        # load_krx_to_redshift_from_s3.delete_wrong_row(schema, table, '"Code" like \'%Code%\'')

        # task_logger.info("Altering columns type")
        # real_column_type = {
        #     "Code": "VARCHAR(300)",
        #     "ISU_CD": "VARCHAR(300)",
        #     "Name": "VARCHAR(300)",
        #     "Market": "VARCHAR(300)",
        #     "Dept": "VARCHAR(300)",
        #     "Close": "BIGINT",
        #     "ChangeCode": "BIGINT",
        #     "Changes": "BIGINT",
        #     "ChangesRatio": "FLOAT",
        #     "Open": "BIGINT",
        #     "High": "BIGINT",
        #     "Low": "BIGINT",
        #     "Volume": "BIGINT",
        #     "Amount": "BIGINT",
        #     "Marcap": "BIGINT",
        #     "Stocks": "BIGINT",
        #     "MarketId": "VARCHAR(300)",
        # }
        # load_krx_to_rds_from_s3.alter_column_type(schema, table, real_column_type)
        trans.commit()
    except Exception as e:
        trans.rollback()
        raise e
    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()
