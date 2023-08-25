# nas_to_s3.py
from ETL_dags.nasdaq.constants import FilePath, AWS
from ETL_dags.common.loadToDL import LoadToDL
from ETL_dags.common.csv import csv_to_rb


def load_nas_stock_data_to_s3(task_logger):
    load = LoadToDL(
        AWS.aws_access_key_id.value,
        AWS.aws_secret_access_key.value,
        AWS.s3_bucket.value,
    )

    task_logger.info("Loading nas list data to S3")
    load.load_to_s3(
        load.create_boto3_object(),
        "nas_list.csv",
        csv_to_rb(FilePath.data_nas_list_csv.value),
    )

    task_logger.info("Loading nas stock data to S3")
    load.load_to_s3(
        load.create_boto3_object(),
        "nas_stock.csv",
        csv_to_rb(FilePath.data_nas_stock_csv.value),
    )
