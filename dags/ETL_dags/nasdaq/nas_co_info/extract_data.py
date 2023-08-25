from ETL_dags.common.extract import Extract
from ETL_dags.nasdaq.constants import FilePath
from ETL_dags.common.csv import df_to_csv
import time
import pandas as pd


def extract_nas_co_info_data(task_logger):
    symbols = [
        "AAPL",
        "MSFT",
        "GOOGL",
        "AMZN",
        "NVDA",
        "META",
        "TSLA,",
        "PEP",
        "AVGO",
        "ASML",
        "AZN",
        "COST",
        "CSCO",
        "CMCSA",
        "TMUS",
        "ADBE",
        "TXN",
        "NFLX",
        "SNY",
        "HON",
        "AMD",
        "INTC",
        "QCOM",
        "AMGN",
        "INTU",
        "ISRG",
        "MDLZ",
        "GILD",
        "BKNG",
        "AMAT",
        "ADI",
        "ADP",
        "VRTX",
        "REGN",
        "PDD",
        "PYPL",
        "ABNB",
        "FISV",
        "LRCX",
        "MU",
        "EQIX",
        "CME",
        "MELI",
        "CSX",
        "MNST",
        "ATVI",
        "ORLY",
        "NETS",
        "CDNS",
        "SNPS",
    ]
    apikeys = ["MLDX4OZKAPYG8ITC", "DOYWPHB2QIYPUC46"]
    nas_co_info_filepath = FilePath.data_nas_co_info_csv.value
    extract_nas = Extract("NASDAQ")

    task_logger.info("Extracting nas info of listed companies")
    raw_df = extract_nas.info_of_listed_companies()
    task_logger.info(raw_df["Sector"].values.tolist())

    task_logger.info("Loading extracted data to csv")
    df_to_csv(raw_df, nas_co_info_filepath)

    all_dataframes = []
    for i in range(len(symbols)):
        if i != 0:
            if i % 5 == 0:
                time.sleep(60)
        df = extract_nas.info_of_listed_companies(symbols[i], apikeys)
        all_dataframes.append(df)
    raw_df = pd.concat(all_dataframes, ignore_index=True)
    raw_df.to_csv(nas_co_info_filepath, mode="w", index=False, header=False)
