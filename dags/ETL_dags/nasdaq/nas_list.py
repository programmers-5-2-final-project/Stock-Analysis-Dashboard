import FinanceDataReader as fdr


def extract():
    df = fdr.StockListing("NASDAQ")
    df.to_csv("./data/nas_list.csv", index=False, encoding="utf-8-sig")
    return
