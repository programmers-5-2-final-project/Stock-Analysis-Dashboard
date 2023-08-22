# nas_list.py
import FinanceDataReader as fdr
import pandas as pd


def extract():
    return fdr.StockListing("NASDAQ")
