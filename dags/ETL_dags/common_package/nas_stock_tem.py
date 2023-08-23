# nas_stock.py
import FinanceDataReader as fdr
import pandas as pd
import time
import concurrent.futures

nas_list_filepath = "./data/nas_list.csv"
nas_stock_filepath = "./data/nas_stock.csv"

# CSV 파일의 column 설정
new_columns = ["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]
df = pd.DataFrame(columns=new_columns)

# 수정된 DataFrame을 다시 CSV 파일로 저장
df.to_csv(nas_stock_filepath, index=False)  # index는 저장하지 않음


def to_nas_stock_csv(symbol):
    df = fdr.DataReader(symbol, "2003")
    df["Symbol"] = symbol
    df_no_duplicates = df[~df.index.duplicated(keep="first")]
    df_no_duplicates.to_csv(nas_stock_filepath, mode="a", index=True, header=False)


if __name__ == "__main__":
    nas_list = pd.read_csv(nas_list_filepath)
    nas_Symbols = list(set(nas_list["Symbol"].tolist()))
    dic = {
        1: nas_Symbols[:1000],
        2: nas_Symbols[1000:2000],
        3: nas_Symbols[2000:3000],
        4: nas_Symbols[3000:],
    }
    with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:
        for i in range(1, 5):
            for j in dic[i]:
                executor.submit(to_nas_stock_csv, j)
                print(j)
            if i != 4:
                print("sleep")
                time.sleep(300)  # 5분 쉬기
