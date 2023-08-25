from ETL_dags.common.transform import Transform
from ETL_dags.common.csv import csv_to_df, df_to_csv
from ETL_dags.nasdaq.constants import FilePath


def transform_nas_co_info_data(task_logger):
    task_logger.info("Reading extracted data from csv")
    raw_df = csv_to_df(FilePath.data_nas_co_info_csv.value)
    task_logger.info("Dropping wrong row of which Code, Name, Sector are NaN")
    transformed_nas = Transform("NASDAQ", raw_df)
    transformed_nas.drop_column([raw_df.columns[3]])
    transformed_nas.drop_nan([raw_df.columns[0]])

    task_logger.info("Loading transformed data to csv")
    df_to_csv(transformed_nas.df, FilePath.data_nas_co_info_t_csv.value, is_new=False)
