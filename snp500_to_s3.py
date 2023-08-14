from datetime import datetime
from airflow import DAG
from airflow.decorators import task, dag
import pandas as pd
import FinanceDataReader as fdr
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.exceptions import NoCredentialsError
from io import StringIO, BytesIO


def fetch_data(symbol):
    records = []
    df = fdr.DataReader(symbol, '2023')
    for index, row in df.iterrows():
        date = index.strftime('%Y-%m-%d')
        records.append([date, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"], symbol])
    
    return pd.DataFrame(records, columns=["Date", "Open", "High", "Low", "Close", "Volume", "Symbol"])


# @task
# def get_dataframe():
#     df_spx = fdr.StockListing('S&P500')
#     symbols = list(df_spx['Symbol'])
#     all_dataframes = []

#     with ThreadPoolExecutor(max_workers=6) as executor:
#         futures = [executor.submit(fetch_data, symbol) for symbol in symbols]
#         all_dataframes = [future.result() for future in futures]

#     final_dataframe = pd.concat(all_dataframes, ignore_index=True)
#     final_dataframe.to_csv("/tmp/final_dataframe.csv", index=False)
   
@task
def get_dataframe_to_s3():

    df_spx = fdr.StockListing('S&P500')
    symbols = list(df_spx['Symbol'])
    all_dataframes = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(fetch_data, symbol) for symbol in symbols]
        all_dataframes = [future.result() for future in futures]

    final_dataframe = pd.concat(all_dataframes, ignore_index=True)
    # final_dataframe.to_csv("/tmp/final_dataframe.csv", index=False)
    # 임의의 데이터 프레임 생성
    # final_dataframe = pd.DataFrame({
    #     'a': [1, 2, 3],
    #     'b': [4, 5, 6]
    # })
    csv_buffer = StringIO()
    final_dataframe.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # 파일 포인터를 시작 위치로 이동
    byte_csv_buffer = BytesIO(csv_buffer.getvalue().encode())

    AWS_ACCESS_KEY = 'AKIA4RRVVY55YXCM2MPC'
    AWS_SECRET_KEY = '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'
    S3_BUCKET = 'de-5-2'
    S3_FILE = 'snp500_final.csv'
    # LOCAL_FILE = "/tmp/final_dataframe.csv"

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    s3.upload_fileobj(byte_csv_buffer, S3_BUCKET, S3_FILE)

with DAG(
    dag_id='SNP500_To_S3',
    start_date=datetime(2023, 8, 10),
    catchup=False,
    tags=['API'],
    schedule_interval='0 10 * * *'
) as dag:

    get_dataframe_to_s3()