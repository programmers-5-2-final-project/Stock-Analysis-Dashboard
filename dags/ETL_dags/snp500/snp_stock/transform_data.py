from ETL_dags.common.transform import Transform
from ETL_dags.common.csv import csv_to_df, df_to_csv
from ETL_dags.snp500.constants import FilePath


def transform_snp_list_data(task_logger):
    task_logger.info("Reading extracted data from csv")
    raw_df = csv_to_df(FilePath.tmp_snp_list_csv.value)

    task_logger.info("Dropping wrong row of which anything is NaN")
    transformed_snp500 = Transform("S&P500", raw_df)
    transformed_snp500.drop_nan(raw_df.columns.values.tolist())

    task_logger.info("Loading transformed data to csv")
    df_to_csv(transformed_snp500.df, FilePath.data_snp_list_csv.value)
