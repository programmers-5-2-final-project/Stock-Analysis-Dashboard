# nas_stock.py
import FinanceDataReader as fdr
import pandas as pd
import time


def extract(task_logger=None):
    nas_list_filepath = "./data/nas_list.csv"
    nas_stock_filepath = "./data/nas_stock.csv"

    nas_list = pd.read_csv(nas_list_filepath)
    nas_Symbols = list(set(nas_list["Symbol"].tolist()))  # 중복제거
    # task_logger.info(nas_Symbols)

    # CSV 파일의 column 설정
    new_columns = ["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]
    df = pd.DataFrame(columns=new_columns)

    # 수정된 DataFrame을 다시 CSV 파일로 저장
    df.to_csv(nas_stock_filepath, index=False)  # index는 저장하지 않음

    def to_nas_stock_csv(symbol):
        # task_logger.info(f"Worker process id for {symbol}")
        try:
            df = fdr.DataReader(symbol, "2003")
            df["Symbol"] = symbol
            df_no_duplicates = df[~df.index.duplicated(keep="first")]
            df_no_duplicates.to_csv(
                nas_stock_filepath, mode="a", index=True, header=False
            )
        except Exception as e:
            # task_logger.info(e)
            print(e)
            pass

    for i in range(0, len(nas_Symbols)):
        if i % 1000 == 0 and i != 0:
            time.sleep(300)
        to_nas_stock_csv(nas_Symbols[i])
        print(nas_Symbols[i], i)


# extract()
