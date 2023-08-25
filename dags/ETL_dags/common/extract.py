# 한국거래소 상장종목 전체
import FinanceDataReader as fdr
import pandas as pd
import random
import requests


class Extract:
    """
    Extract task에서 사용하는 클래스,
    일단 krx를 위주로 구성
    """

    def __init__(self, market: str):
        self.market = market

    def values_of_listed_companies(self) -> pd.DataFrame:
        """
        상장된 기업들의 시가총액, 주식거래량, 전날 주식값을 추출
        """
        print(fdr.__file__)
        return fdr.StockListing(self.market)

    def info_of_listed_companies(self, symbol=None, apikeys=None) -> pd.DataFrame:
        """
        상장된 기업들의 섹터 같은 일반적인 정보를 추출
        """
        if self.market in ["KRX", "KOSPI", "KOSDAQ", "KONEX"]:
            return fdr.StockListing(self.market + "-DESC")
        elif self.market in ["NASDAQ", "S&P500"] and symbol and apikeys:
            apikey = random.choice(apikeys)
            url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={apikey}"

            try:
                r = requests.get(url)
                r.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
                data = r.json()
                if "Error Message" in data:
                    # Check if the API returned an error message
                    raise FetchDataError(data["Error Message"])
                return pd.DataFrame([data])

            except requests.RequestException as e:
                # This will catch any request-related exceptions (like connection errors)
                raise FetchDataError(
                    f"Failed to fetch data for symbol {symbol}. Error: {str(e)}"
                )
        return False

    def stock_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        기업에 대한 주식 데이터를 추출
        """
        return fdr.DataReader(code, start_date, end_date)


class FetchDataError(Exception):
    """Custom error for fetch_data function."""

    pass
