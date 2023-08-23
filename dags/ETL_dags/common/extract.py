# 한국거래소 상장종목 전체
import FinanceDataReader as fdr
import pandas as pd
from enum import Enum


class Extract:
    """
    Extract task에서 사용하는 클래스
    """

    def __init__(self, market: str):
        self.market = market

    def values_of_listed_companies(self) -> pd.DataFrame:
        """
        상장된 기업들의 시가총액, 주식거래량, 전날 주식값을 추출
        """
        return fdr.StockListing(self.market)

    def info_of_listed_companies(self) -> pd.DataFrame:
        """
        상장된 기업들의 섹터 같은 일반적인 정보를 추출
        """
        if self.market in ["KRX", "KOSPI", "KOSDAQ", "KONEX"]:
            return fdr.StockListing(self.market + "-DESC")
        return False

    def stock_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        기업에 대한 주식 데이터를 추출
        """
        return fdr.DataReader(code, start_date, end_date)
