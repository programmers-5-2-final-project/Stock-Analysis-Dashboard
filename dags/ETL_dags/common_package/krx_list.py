# 한국거래소 상장종목 전체
import FinanceDataReader as fdr
import pandas as pd


def extract():
    return fdr.StockListing("KRX")
