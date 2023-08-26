from ETL_dags.common.loadToDW import LoadToDW
from ETL_dags.common.db import DB
from ETL_dags.krx.constants import RDS, AWS
import sqlalchemy


def load_nas_co_info_data_to_rds_from_s3(task_logger):
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
    load_nas_to_rds_from_s3 = LoadToDW(db.engine)
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
            "Symbol": "VARCHAR(100)",
            "AssetType": "VARCHAR(100)",
            "Name": "VARCHAR(100)",
            "CIK": "VARCHAR(100)",
            "Exchange": "VARCHAR(100)",
            "Currency": "VARCHAR(100)",
            "Country": "VARCHAR(100)",
            "Sector": "VARCHAR(100)",
            "Industry": "VARCHAR(100)",
            "Address": "VARCHAR(100)",
            "FiscalYearEnd": "VARCHAR(100)",
            "LatestQuarter": "VARCHAR(100)",
            "MarketCapitalization": "VARCHAR(100)",
            "EBITDA": "VARCHAR(100)",
            "PERatio": "VARCHAR(100)",
            "PEGRatio": "VARCHAR(100)",
            "BookValue": "VARCHAR(100)",
            "DividendPerShare": "VARCHAR(100)",
            "DividendYield": "VARCHAR(100)",
            "EPS": "VARCHAR(100)",
            "RevenuePerShareTTM": "VARCHAR(100)",
            "ProfitMargin": "VARCHAR(100)",
            "OperatingMarginTTM": "VARCHAR(100)",
            "ReturnOnAssetsTTM": "VARCHAR(100)",
            "ReturnOnEquityTTM": "VARCHAR(100)",
            "RevenueTTM": "VARCHAR(100)",
            "GrossProfitTTM": "VARCHAR(100)",
            "DilutedEPSTTM": "VARCHAR(100)",
            "QuarterlyEarningsGrowthYOY": "VARCHAR(100)",
            "QuarterlyRevenueGrowthYOY": "VARCHAR(100)",
            "AnalystTargetPrice": "VARCHAR(100)",
            "TrailingPE": "VARCHAR(100)",
            "ForwardPE": "VARCHAR(100)",
            "PriceToSalesRatioTTM": "VARCHAR(100)",
            "PriceToBookRatio": "VARCHAR(100)",
            "EVToRevenue": "VARCHAR(100)",
            "EVToEBITDA": "VARCHAR(100)",
            "Beta": "VARCHAR(100)",
            "Week52High": "VARCHAR(100)",
            "Week52Low": "VARCHAR(100)",
            "Day50MovingAverage": "VARCHAR(100)",
            "Day200MovingAverage": "VARCHAR(100)",
            "SharesOutstanding": "VARCHAR(100)",
            "DividendDate": "VARCHAR(100)",
            "ExDividendDate": "VARCHAR(100)",
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
