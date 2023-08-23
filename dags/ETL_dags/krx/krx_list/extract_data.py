from ETL_dags.common.extract import Extract
from ETL_dags.krx.constants import FilePath
from ETL_dags.common.csv import df_to_csv


def extract_krx_list_data(task_logger):
    extract_krx = Extract("KRX")

    task_logger.info("Extracting krx values of listed companies")
    raw_df = extract_krx.values_of_listed_companies()

    task_logger.info("Loading extracted data to csv")
    df_to_csv(raw_df, FilePath.tmp_krx_list_csv.value)
