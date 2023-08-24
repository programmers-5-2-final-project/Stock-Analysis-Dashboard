from ETL_dags.common.csv import csv_to_df, df_to_csv
from ETL_dags.krx.constants import FilePath
from ETL_dags.common.transform import Transform


def transform_krx_stock_data(task_logger):
    print("@#@$@324Test")

    task_logger.info("Reading krx stock csv")
    raw_df = csv_to_df(FilePath.tmp_krx_stock_csv.value)

    task_logger.info("Deleting Change column")
    transformed_krx = Transform("KRX", raw_df)
    transformed_krx.drop_column(["Change"])

    task_logger.info("Deleting wrong row of which Date, Code is NaN")
    transformed_krx.drop_nan(["Date", "Code"])

    task_logger.info("Filling Nan with forward value")
    transformed_krx.fill_nan()

    task_logger.info("Formatting Code")
    transformed_krx.format_column_zfill("Code", 6)

    task_logger.info("Loading transformed data to csv")
    df_to_csv(transformed_krx.df, FilePath.data_krx_stock_csv.value)
