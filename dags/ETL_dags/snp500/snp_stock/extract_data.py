from ETL_dags.common.extract import Extract
from ETL_dags.snp500.constants import FilePath
from ETL_dags.common.csv import df_to_csv, csv_to_df
import pandas as pd
import FinanceDataReader as fdr
from concurrent.futures import ThreadPoolExecutor


def extract_snp_stock_data():
    snp_list = csv_to_df(FilePath.data_snp_list_csv.value)  # 저장된 기업리스트 불러옴
    symbols = snp_list["Symbol"].tolist()

    all_dataframes = []

    def fetch_data(symbol):
        records = []
        df = fdr.DataReader(symbol, 2023)
        for index, row in df.iterrows():
            date = index.strftime("%Y-%m-%d")
            records.append(
                [
                    date,
                    row["Open"],
                    row["High"],
                    row["Low"],
                    row["Close"],
                    row["Volume"],
                    symbol,
                ]
            )

        return pd.DataFrame(
            records,
            columns=["Date", "Open", "High", "Low", "Close", "Volume", "Symbol"],
        )

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(fetch_data, symbol) for symbol in symbols]
        for future in futures:
            all_dataframes.append(future.result())

    raw_df = pd.concat(all_dataframes, ignore_index=True)
    raw_df.to_csv(f"./tmp/snp_stock.csv", index=False)

    return True
