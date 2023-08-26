import pandas as pd


def df_to_csv(
    df: pd.DataFrame,
    file_path: str,
    index: bool = False,
    header: bool = False,
    is_new: bool = True,
    encoding: str = "utf-8-sig",
) -> None:
    if is_new:
        new_columns = df.columns.values.tolist()
        init_df = pd.DataFrame(columns=new_columns)
        init_df.to_csv(file_path, mode="w", index=index, encoding=encoding)
        df.to_csv(file_path, mode="a", index=index, header=header, encoding=encoding)
    else:
        df.to_csv(file_path, mode="w", index=index, header=header, encoding=encoding)


def csv_to_df(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)


def csv_to_rb(file_path: str):
    return open(file_path, "rb")
