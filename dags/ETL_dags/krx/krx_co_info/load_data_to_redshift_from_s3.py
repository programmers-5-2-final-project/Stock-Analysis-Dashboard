from ETL_dags.common.loadToDW import LoadToRedshift
from ETL_dags.common.db import DB
from ETL_dags.krx.constants import REDSHIFT, AWS 
import sqlalchemy


def load_krx_co_info_data_to_redshift_from_s3(task_logger):
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
    load_krx_to_redshift_from_s3 = LoadToRedshift(db.conn)

    # 트랜잭션 시작
    trans = db.conn.begin()
    try:
        schema = "raw_data"
        table = "krx_co_info"

        task_logger.info("Dropping existing raw_data.krx_co_info")
        load_krx_to_redshift_from_s3.drop_table(schema, table)

        task_logger.info("Creating the table raw_data.krx_co_info")
        tmp_column_type = {
            "Code": "VARCHAR(300)",
            "Name": "VARCHAR(300)",
            "Market": "VARCHAR(300)",
            "Sector": "VARCHAR(300)",
            "Industry": "VARCHAR(300)",
            "ListingDate": "VARCHAR(300)",
            "SettleMonth": "VARCHAR(300)",
            "Representative": "VARCHAR(300)",
            "HomePage": "VARCHAR(300)",
            "Region": "VARCHAR(300)",
        }
        primary_key = '"Code"'
        load_krx_to_redshift_from_s3.create_table(
            schema, table, tmp_column_type, primary_key
        )

        task_logger.info("Importing from s3")
        load_krx_to_redshift_from_s3.table_import_from_s3(
            schema, table, AWS.s3_bucket.value, "krx_co_info.csv"
        )

        task_logger.info("Deleting wrong row")
        load_krx_to_redshift_from_s3.delete_wrong_row(schema, table, '"Code" like \'%Code%\'')

        # task_logger.info("Altering columns type")
        # real_column_type = {
        #     "Code": "VARCHAR(40)",
        #     "Name": "VARCHAR(300)",
        #     "Market": "VARCHAR(40)",
        #     "Sector": "VARCHAR(300)",
        #     "Industry": "VARCHAR(300)",
        #     "ListingDate": "TIMESTAMP",
        #     "SettleMonth": "VARCHAR(40)",
        #     "Representative": "VARCHAR(300)",
        #     "HomePage": "VARCHAR(300)",
        #     "Region": "VARCHAR(40)",
        # }
        # load_krx_to_redshift_from_s3.alter_column_type(schema, table, real_column_type)
        trans.commit()
    except Exception as e:
        trans.rollback()
        raise e
    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()
