import pandas as pd


def df_to_csv(df: pd.DataFrame, file_path: str) -> None:
    new_columns = df.columns.values.tolist()
    init_df = pd.DataFrame(columns=new_columns)
    init_df.to_csv(file_path, index=False, encoding="utf-8-sig")
    df.to_csv(file_path, mode="a", index=False, header=False, encoding="utf-8-sig")


def csv_to_df(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)


def csv_to_rb(file_path: str):
    return open(file_path, "rb")