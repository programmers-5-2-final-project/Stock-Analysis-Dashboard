from ETL_dags.nasdaq.constants import AWS, FilePath
from ETL_dags.common.loadToDL import LoadToDL
from ETL_dags.common.csv import csv_to_rb


def load_nas_co_info_data_to_s3(task_logger):
    load_nas_to_s3 = LoadToDL(
        AWS.aws_access_key_id.value,
        AWS.aws_secret_access_key.value,
        AWS.s3_bucket.value,
    )

    task_logger.info("Loading transformed data to S3")
    load_nas_to_s3.load_to_s3(
        load_nas_to_s3.create_boto3_object(),
        "nas_co_info.csv",
        csv_to_rb(FilePath.data_nas_co_info_t_csv.value),
    )
