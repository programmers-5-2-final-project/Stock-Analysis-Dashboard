import pandas as pd


class Transform:
    def __init__(self, market: str, df: pd.DataFrame):
        self.market = market
        self.df = df

    def drop_nan(self, columns: list):
        self.df.dropna(subset=columns)
