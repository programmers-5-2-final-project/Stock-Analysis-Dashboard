# nas_partition_of_stock_by_code.py


import boto3
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import dotenv_values


def extract_symbol_list():
    resultproxy = engine.execute(
        text("SELECT DISTINCT symbol FROM raw_data.nas_stock;")
    )
    symbol_list = []
    for rowproxy in resultproxy:
        for _, symbol in rowproxy.items():
            symbol_list.append(symbol)
    return symbol_list


def create_table(symbol_list):
    for symbol in symbol_list:
        engine.execute(
            text(
                f"""
                    DROP TABLE IF EXISTS analytics.nas_stock_{symbol};
                        """
            )
        )

    engine.execute(
        text(
            """
                    DROP TABLE IF EXISTS analytics.nas_partition_of_stock_by_symbol;
                        """
        )
    )

    print("create table analytics.nas_partition_of_stock_by_symbol")
    engine.execute(
        text(
            """
                CREATE TABLE analytics.nas_partition_of_stock_by_symbol(
                        Date Date,
                        Open FLOAT,
                        High FLOAT,
                        Low FLOAT,
                        Close FLOAT,
                        Adj_Close FLOAT,
                        Volume FLOAT,
                        Symbol VARCHAR(40),
                        CONSTRAINT PK_nas_partition_of_stock_by_symbol PRIMARY KEY(date, symbol)
                ) PARTITION BY LIST(symbol);
            """
        )
    )
    return symbol_list


def create_partitioned_tables(symbol_list):
    for symbol in symbol_list:
        print(f"create partitioned analytics.nas_stock_{symbol}")
        engine.execute(
            text(
                f"""
                    CREATE TABLE analytics.nas_stock_{symbol}
                        PARTITION OF analytics.nas_partition_of_stock_by_symbol
                        FOR VALUES IN ('{symbol}');
                        """
            )
        )

    return True


def insert_into_table(_):
    print("insert into table from raw_data.nas_stock")
    engine.execute(
        text(
            """
                    INSERT INTO analytics.nas_partition_of_stock_by_symbol
                        SELECT * FROM raw_data.nas_stock;
                        """
        )
    )
    return True


if __name__ == "__main__":
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ

    # connect RDS
    connection_uri = "postgresql://{}:{}@{}:{}/{}".format(
        CONFIG["POSTGRES_USER"],
        CONFIG["POSTGRES_PASSWORD"],
        CONFIG["POSTGRES_HOST"],
        CONFIG["POSTGRES_PORT"],
        CONFIG["POSTGRES_DB"],
    )
    engine = create_engine(
        connection_uri, pool_pre_ping=True, isolation_level="AUTOCOMMIT"
    )
    conn = engine.connect()

    insert_into_table(create_partitioned_tables(create_table(extract_symbol_list())))

    # close RDS
    conn.close()
    engine.dispose()
