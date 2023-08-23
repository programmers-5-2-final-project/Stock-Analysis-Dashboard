# nas_list.py
import FinanceDataReader as fdr
import pandas as pd


def extract():
    df = fdr.StockListing("NASDAQ")
    df.to_csv("./data/nas_list.csv", index=False, encoding="utf-8-sig")
    return


# extract()


def check():
    df = fdr.StockListing("NASDAQ")
    l = df["Symbol"].tolist()
    print(len(l))
    print(len(set(l)))
    for i in range(len(l)):
        for j in range(i + 1, len(l)):
            if l[i] == l[j]:
                print(l[i])
                print(i)
                print(j)
