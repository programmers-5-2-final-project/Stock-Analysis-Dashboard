from ETL_dags.common.extract import Extract
from ETL_dags.krx.constants import FilePath
from ETL_dags.common.csv import df_to_csv
import pandas as pd


def extract_krx_list_data() -> pd.DataFrame:
    extract_krx = Extract("KRX")

    raw_df = extract_krx.values_of_listed_companies()

    return raw_df


def extracted_data_to_csv(raw_df: pd.DataFrame):
    df_to_csv(raw_df, FilePath.tmp_krx_list_csv.value)
