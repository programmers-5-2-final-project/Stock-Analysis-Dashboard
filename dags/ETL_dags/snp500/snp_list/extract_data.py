from ETL_dags.common.extract import Extract
from ETL_dags.snp500.constants import FilePath
from ETL_dags.common.csv import df_to_csv


def extract_snp_list_data(task_logger):
    extract_snp500 = Extract("S&P500")

    task_logger.info("Extracting snp500 values of listed companies")
    raw_df = extract_snp500.values_of_listed_companies()

    task_logger.info("Loading extracted data to csv")
    df_to_csv(raw_df, FilePath.tmp_snp_list_csv.value)
