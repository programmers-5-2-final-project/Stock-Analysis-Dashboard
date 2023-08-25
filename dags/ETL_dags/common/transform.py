import pandas as pd


class Transform:
    def __init__(self, market: str, df: pd.DataFrame):
        self.market = market
        self.df = df

    def drop_nan(self, columns: list):
        self.df.dropna(subset=columns, inplace=True)

    def drop_column(self, columns: list):
        self.df.drop(columns=columns, inplace=True)

    def fill_nan(self, column=None, value=None, method=None):
        if column:
            self.df[column].fillna(value, method, inplace=True)
        else:
            self.df.fillna(value, method, inplace=True)

    def format_column_zfill(self, column, num):
        self.df[column] = self.df[column].apply(lambda x: str(x).zfill(num))

    def column_to_numeric(self, column):
        self.df[column] = pd.to_numeric(self.df[column], errors="coerce")

    def drop_duplicates(self):
        self.df.drop_duplicates()

    def pct_change(self, column_Y, column_x):
        self.df[column_Y] = self.df[column_x].pct_change()
