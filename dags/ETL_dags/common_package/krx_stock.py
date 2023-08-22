# 한국거래소 상장종목 전체
import FinanceDataReader as fdr
import pandas as pd
import concurrent.futures
import os

krx_list_filepath = "/data/krx_list.csv"
krx_stock_filepath = "/data/krx_stock.csv"


def to_krx_stock_csv(code):
    print(f"Worker process id for {code}:{os.getpid()}")
    try:
        df = fdr.DataReader(code, "2003")
        df["Code"] = code
        df.to_csv(krx_stock_filepath, mode="a", index=True, header=False)
    except Exception as error:
        print(error)


if __name__ == "__main__":
    krx_list = pd.read_csv(krx_list_filepath)
    krx_codes = krx_list["Code"].tolist()
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
        for code in krx_codes:
            executor.submit(to_krx_stock_csv, code)
