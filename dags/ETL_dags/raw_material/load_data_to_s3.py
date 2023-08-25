from ETL_dags.raw_material.constants import AWS, FilePath
from ETL_dags.common.loadToDL import LoadToDL
from ETL_dags.common.csv import csv_to_rb


def load_raw_material_price_data_to_s3(task_logger, raw_material):
    load_raw_material_to_s3 = LoadToDL(
        AWS.aws_access_key_id.value,
        AWS.aws_secret_access_key.value,
        AWS.s3_bucket.value,
    )

    if raw_material == "gold":
        file_path = FilePath.data_gold_price_csv.value
    if raw_material == "silver":
        file_path = FilePath.data_silver_price_csv.value
    if raw_material == "cme":
        file_path = FilePath.data_cme_price_csv.value
    if raw_material == "orb":
        file_path = FilePath.data_orb_price_csv.value

    task_logger.info("Loading transformed data to S3")
    load_raw_material_to_s3.load_to_s3(
        load_raw_material_to_s3.create_boto3_object(),
        f"{raw_material}_price.csv",
        csv_to_rb(file_path),
    )
