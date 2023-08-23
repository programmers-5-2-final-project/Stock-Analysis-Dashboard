from ETL_dags.common.transform import Transform
from ETL_dags.common.csv import csv_to_df, df_to_csv
from ETL_dags.krx.constants import FilePath


def transform_krx_co_info_data(task_logger):
    task_logger.info("Reading extracted data from csv")
    raw_df = csv_to_df(FilePath.tmp_krx_co_info_csv.value)

    task_logger.info("Dropping wrong row of which Code, Name, Sector are NaN")
    transformed_krx = Transform("KRX", raw_df)
    transformed_krx.drop_nan(["Code", "Name", "Sector"])

    task_logger.info("Loading transformed data to csv")
    df_to_csv(transformed_krx.df, FilePath.data_krx_co_info_csv.value)
