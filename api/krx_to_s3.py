import boto3
import os

s3 = boto3.resource("s3")

krx_list_data = open(
    "/home/sanso/FinalProject/Stock-Analysis-Dashboard/api/data/krx_list.csv", "rb"
)
krx_stock_data = open(
    "/home/sanso/FinalProject/Stock-Analysis-Dashboard/api/data/krx_stock.csv", "rb"
)

try:
    s3.Bucket("de-5-2").put_object(Key="krx_list.csv", Body=krx_list_data)
    s3.Bucket("de-5-2").put_object(Key="krx_stock.csv", Body=krx_stock_data)
except error:
    print(error)
