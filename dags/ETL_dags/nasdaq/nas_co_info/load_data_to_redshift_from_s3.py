from ETL_dags.common.loadToDW import LoadToRedshift
from ETL_dags.common.db import DB
from ETL_dags.krx.constants import RDS, AWS
import sqlalchemy


def load_nas_co_info_data_to_redshift_from_s3(task_logger):
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
    load_nas_to_rds_from_s3 = LoadToRedshift(db.conn)
    try:
        task_logger.info("Installing the aws_s3 extension")
        load_nas_to_rds_from_s3.install_aws_s3_extension()
    except sqlalchemy.exc.ProgrammingError:
        task_logger.info("aws_s3 extension already exists")

    # 트랜잭션 시작
    trans = db.conn.begin()
    try:
        schema = "raw_data"
        table = "nas_co_info"

        task_logger.info("Dropping existing raw_data.nas_co_info")
        load_nas_to_rds_from_s3.drop_table(schema, table)

        task_logger.info("Creating the table raw_data.nas_co_info")
        tmp_column_type = {
            "Symbol": "VARCHAR(300)",
            "AssetType": "VARCHAR(300)",
            "Name": "VARCHAR(300)",
            "CIK": "VARCHAR(300)",
            "Exchange": "VARCHAR(300)",
            "Currency": "VARCHAR(300)",
            "Country": "VARCHAR(300)",
            "Sector": "VARCHAR(300)",
            "Industry": "VARCHAR(300)",
            "Address": "VARCHAR(300)",
            "FiscalYearEnd": "VARCHAR(300)",
            "LatestQuarter": "VARCHAR(300)",
            "MarketCapitalization": "VARCHAR(300)",
            "EBITDA": "VARCHAR(300)",
            "PERatio": "VARCHAR(300)",
            "PEGRatio": "VARCHAR(300)",
            "BookValue": "VARCHAR(300)",
            "DividendPerShare": "VARCHAR(300)",
            "DividendYield": "VARCHAR(300)",
            "EPS": "VARCHAR(300)",
            "RevenuePerShareTTM": "VARCHAR(300)",
            "ProfitMargin": "VARCHAR(300)",
            "OperatingMarginTTM": "VARCHAR(300)",
            "ReturnOnAssetsTTM": "VARCHAR(300)",
            "ReturnOnEquityTTM": "VARCHAR(300)",
            "RevenueTTM": "VARCHAR(300)",
            "GrossProfitTTM": "VARCHAR(300)",
            "DilutedEPSTTM": "VARCHAR(300)",
            "QuarterlyEarningsGrowthYOY": "VARCHAR(300)",
            "QuarterlyRevenueGrowthYOY": "VARCHAR(300)",
            "AnalystTargetPrice": "VARCHAR(300)",
            "TrailingPE": "VARCHAR(300)",
            "ForwardPE": "VARCHAR(300)",
            "PriceToSalesRatioTTM": "VARCHAR(300)",
            "PriceToBookRatio": "VARCHAR(300)",
            "EVToRevenue": "VARCHAR(300)",
            "EVToEBITDA": "VARCHAR(300)",
            "Beta": "VARCHAR(300)",
            "Week52High": "VARCHAR(300)",
            "Week52Low": "VARCHAR(300)",
            "Day50MovingAverage": "VARCHAR(300)",
            "Day200MovingAverage": "VARCHAR(300)",
            "SharesOutstanding": "VARCHAR(300)",
            "DividendDate": "VARCHAR(300)",
            "ExDividendDate": "VARCHAR(300)",
        }
        primary_key = "Symbol"
        load_nas_to_rds_from_s3.create_table(
            schema, table, tmp_column_type, primary_key
        )

        task_logger.info("Importing from s3")
        load_nas_to_rds_from_s3.table_import_from_s3(
            schema,
            table,
            AWS.s3_bucket.value,
            "nas_co_info.csv",
            AWS.region.value,
            AWS.aws_access_key_id.value,
            AWS.aws_secret_access_key.value,
        )
        trans.commit()
    except Exception as e:
        trans.rollback()
        raise e

    task_logger.info("Closing connection")
    db.close_connection()

    task_logger.info("Disposing sqlalchemy engine")
    db.dispose_sqlalchemy_engine()
