from ETL_dags.common.extract import Extract
from ETL_dags.common.csv import df_to_csv, csv_to_df
from ETL_dags.nasdaq.constants import FilePath
import FinanceDataReader as fdr
import pandas as pd
import time


def extract_nas_list_data(task_logger):
    extract = Extract("NASDAQ")
    df = extract.values_of_listed_companies()
    df_to_csv(
        df,
        FilePath.data_nas_list_csv.value,
        index=False,
        header=True,
    )
    return


def extract_nas_stock_data(task_logger=None):
    extract = Extract("NASDAQ")
    nas_list_filepath = FilePath.data_nas_list_csv.value
    nas_stock_filepath = FilePath.data_nas_stock_csv.value

    nas_list = csv_to_df(nas_list_filepath)
    nas_Symbols = list(set(nas_list["Symbol"].tolist()))  # 중복제거
    task_logger.info(nas_Symbols)

    # CSV 파일의 column 설정
    new_columns = ["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]
    df = pd.DataFrame(columns=new_columns)

    # 수정된 DataFrame을 다시 CSV 파일로 저장
    df.to_csv(nas_stock_filepath, index=False)  # index는 저장하지 않음

    for i in range(0, len(nas_Symbols)):
        if i % 1000 == 0 and i != 0:
            time.sleep(180)
        to_nas_stock_csv(nas_Symbols[i])
        print(nas_Symbols[i], i)


def to_nas_stock_csv(symbol):
    print(f"Worker process id for {symbol}")
    try:
        df = fdr.DataReader(symbol, "2003")
        df["Symbol"] = symbol
        df_no_duplicates = df[~df.index.duplicated(keep="first")]
        df_no_duplicates.to_csv(
            FilePath.data_nas_stock_csv.value, mode="a", index=True, header=False
        )
    except Exception as e:
        print(e)
