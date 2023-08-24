import pandas as pd


class Transform:
    def __init__(self, market: str, df: pd.DataFrame):
        self.market = market
        self.df = df

    def drop_nan(self, columns: list):
        self.df.dropna(subset=columns, inplace=True)

    def drop_column(self, columns: list):
        self.df.drop(columns=columns, inplace=True)

    def fill_nan(self):
        self.df.fillna(method="ffill", inplace=True)

    def format_column_zfill(self, column, num):
        self.df[column] = self.df[column].apply(lambda x: str(x).zfill(num))
