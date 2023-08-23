from ETL_dags.common.transform import Transform
from ETL_dags.common.csv import csv_to_df, df_to_csv
from ETL_dags.krx.constants import FilePath
import pandas as pd


def extracted_data_from_csv(df: pd.DataFrame):
    return csv_to_df(FilePath.tmp_krx_list_csv.value)


def transform_krx_list_data(df: pd.DataFrame):
    transformed_krx = Transform("KRX", df)
    transformed_krx.drop_nan(["Code", "Name"])
    return transformed_krx.df


def transformed_data_to_csv(transformed_df: pd.DataFrame):
    df_to_csv(transformed_df, FilePath.data_krx_list_csv.value)
