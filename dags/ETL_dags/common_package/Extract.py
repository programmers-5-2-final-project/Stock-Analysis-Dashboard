# 한국거래소 상장종목 전체
import FinanceDataReader as fdr
import pandas as pd
from enum import Enum


class Extract:
    def __init__(self, market: str):
        self.market = market

    def extracting_values_of_listed_companies(self) -> pd.DataFrame:
        return fdr.StockListing(self.market)

    def extracting_info_of_listed_companies(self) -> pd.DataFrame:
        return fdr.StockListing(self.market + "-DESC")

    def extracting_stock_data(
        self, code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        return fdr.DataReader(code, start_date, end_date)
    
    def extracting_all_stock_data(self, )
