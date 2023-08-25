from ETL_dags.snp500.constants import AWS, FilePath
from ETL_dags.common.loadToDL import LoadToDL
from ETL_dags.common.csv import csv_to_rb


def load_snp_list_data_to_s3(task_logger):
    S3_FILE = "snp_stock_list.csv"
    LOCAL_FILE = FilePath.data_snp_list_csv.value
    load_snp500_to_s3 = LoadToDL(
        AWS.aws_access_key_id.value,
        AWS.aws_secret_access_key.value,
        AWS.s3_bucket.value,
    )

    task_logger.info("Loading transformed data to S3")
    load_snp500_to_s3.load_to_s3(
        load_snp500_to_s3.create_boto3_object(),
        S3_FILE,
        csv_to_rb(LOCAL_FILE),
    )
