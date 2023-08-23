import boto3
from dags.ETL_dags.krx.constants import AWS, FilePath
from dags.ETL_dags.common.loadToDL import LoadToDL
from dags.ETL_dags.common.csv import csv_to_rb


def load_krx_list_data_to_s3():
    load_krx_to_s3 = LoadToDL(
        AWS.aws_access_key_id.value,
        AWS.aws_secret_access_key.value,
        AWS.s3_bucket.value,
    )

    load_krx_to_s3.load_to_s3(
        load_krx_to_s3.create_boto3_object(),
        "krx_list.csv",
        csv_to_rb(FilePath.data_krx_list_csv.value),
    )
