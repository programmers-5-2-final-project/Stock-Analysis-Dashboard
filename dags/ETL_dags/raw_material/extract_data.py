from ETL_dags.common.extract import Extract
from ETL_dags.common.transform import Transform
from ETL_dags.raw_material.constants import FilePath, Ticker
from ETL_dags.common.csv import df_to_csv


def extract_raw_material_price_data(task_logger, raw_material, start_date, end_date):
    extract_raw_material = Extract(raw_material)

    task_logger.info("Extracting raw_material price")
    raw_df = extract_raw_material.price(start_date, end_date)

    task_logger.info("Transforming extracted data")
    transform_raw_material = Transform(raw_material, raw_df)
    if raw_material == "gold":
        transform_raw_material.drop_nan(["USD (PM)"])
    if raw_material == "silver":
        transform_raw_material.drop_nan(["USD"])
    if raw_material == "cme":
        transform_raw_material.drop_nan(["Open", "High", "Low", "Last", "Settle"])
    if raw_material == "orb":
        transform_raw_material.drop_nan(["Value"])

    task_logger.info("Loading extracted data to csv")
    if raw_material == "gold":
        file_path = FilePath.data_gold_price_csv.value
    if raw_material == "silver":
        file_path = FilePath.data_silver_price_csv.value
    if raw_material == "cme":
        file_path = FilePath.data_cme_price_csv.value
    if raw_material == "orb":
        file_path = FilePath.data_orb_price_csv.value
    df_to_csv(
        raw_df, file_path, index=True, header=False, is_new=False, encoding="cp949"
    )
