from ETL_dags.common.transform import Transform
from ETL_dags.common.csv import csv_to_df, df_to_csv
from ETL_dags.snp500.constants import FilePath


def transform_snp_stock_data(task_logger):
    task_logger.info("Reading extracted data from csv")
    raw_df = csv_to_df(FilePath.tmp_snp_stock_csv.value)

    transformed_snp500 = Transform("S&P500", raw_df)

    task_logger.info("Changing columns to numeric")
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        transformed_snp500.column_to_numeric(col)

    task_logger.info("Dropping wrong row of which anything is NaN")
    transformed_snp500.drop_nan(["Open", "High", "Low", "Close", "Volume"])

    task_logger.info("Dropping duplicates")
    transformed_snp500.drop_duplicates()

    task_logger.info("Add Change column")
    transformed_snp500.pct_change(column_y="Change", column_x="Close")

    task_logger.info("Filling nan value")
    transformed_snp500.fill_nan(column="Change", value=0)

    task_logger.info("Loading transformed data to csv")
    df_to_csv(transformed_snp500.df, FilePath.data_snp_list_csv.value)
