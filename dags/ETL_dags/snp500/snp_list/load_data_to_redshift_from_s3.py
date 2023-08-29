from ETL_dags.common.loadToDW import LoadToRedshift
from ETL_dags.common.db import DB
from ETL_dags.snp500.constants import REDSHIFT, AWS
import sqlalchemy


def load_snp_list_data_to_redshift_from_s3(task_logger):
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
    load_snp500_to_rds_from_s3 = LoadToRedshift(db.conn)

    # 트랜잭션의 시작
    trans = db.conn.begin()
    try:
        schema = "raw_data"
        table = "snp_stock_list"

        task_logger.info("Dropping existing raw_data.snp_stock_list")
        load_snp500_to_rds_from_s3.drop_table(schema, table)

        task_logger.info("Creating the table raw_data.snp_stock_list")
        tmp_column_type = {
            "Symbol": "VARCHAR(300)",
            "Name": "VARCHAR(300)",
            "Sector": "VARCHAR(300)",
            "Industry": "VARCHAR(300)",
        }
        primary_key = '"Symbol"'
        load_snp500_to_rds_from_s3.create_table(
            schema, table, tmp_column_type, primary_key
        )

        task_logger.info("Importing from s3")
        load_snp500_to_rds_from_s3.table_import_from_s3(
            schema,
            table,
            AWS.s3_bucket.value,
            "snp_stock_list.csv",
        )

        task_logger.info("Deleting wrong row")
        load_snp500_to_rds_from_s3.delete_wrong_row(
            schema, table, "\"Symbol\" like '%Symbol%'"
        )
        # 트랜잭션 종료
        trans.commit()

    except Exception as e:
        trans.rollback()
        task_logger.error(f"오류 발생: {e}")
        raise

    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()
