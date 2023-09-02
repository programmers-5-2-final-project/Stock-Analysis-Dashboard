import pandas as pd
import numpy as np
import sqlalchemy
import tensorflow as tf
import os
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
from dotenv import dotenv_values
from sqlalchemy import text
from tensorflow.keras.callbacks import LearningRateScheduler
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import logging


task_logger = logging.getLogger("airflow.task")

CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ

# PostgreSQL RDS 연결 정보 설정
db_host = CONFIG["RDS_HOST"]
db_port = CONFIG["RDS_PORT"]
db_name = "dev"
db_user = CONFIG["RDS_USER"]
db_password = CONFIG["RDS_PASSWORD"]

# SQLAlchemy 연결 URL 생성
db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = sqlalchemy.create_engine(db_url)


def load_data_from_db(query):
    conn = engine.connect()
    data = pd.read_sql(query, conn)
    conn.close()
    return data


@task
def snp_ml():
    task_logger.info("snp_ml")
    new_columns = ["symbol", "tomorrow_close", "up/down", "diff"]
    df = pd.DataFrame(columns=new_columns)
    data_l = []

    def predict_tomorrow_stock_price(symbol):
        try:
            data_query = f"SELECT date, open, close, high, low, volume FROM analytics.snp_stock_{symbol}"
            data = load_data_from_db(data_query)

            data["date"] = pd.to_datetime(data["date"])
            data = data.sort_values("date")

            data["daily_return"] = data["close"].pct_change()
            data.dropna(inplace=True)
            scaler = StandardScaler()
            data["volume"] = scaler.fit_transform(data[["volume"]])

            def build_recursive_model(input_shape):
                model = tf.keras.Sequential(
                    [
                        tf.keras.layers.LSTM(
                            50, activation="relu", input_shape=input_shape
                        ),
                        tf.keras.layers.Dense(1),
                    ]
                )
                model.compile(optimizer="adam", loss="mean_squared_error")
                return model

            X = data[["open", "close", "high", "low", "volume", "daily_return"]].values[
                :-1
            ]
            y = data["close"].values[1:]

            X = X.reshape((X.shape[0], 1, X.shape[1]))

            model = build_recursive_model((X.shape[1], X.shape[2]))

            def lr_schedule(epoch, lr):
                if epoch < 10:
                    return lr
                else:
                    return lr * tf.math.exp(-0.1)

            lr_scheduler = LearningRateScheduler(lr_schedule)
            model.fit(
                X, y, epochs=20, batch_size=16, verbose=1, callbacks=[lr_scheduler]
            )

            today_close = data["close"].iloc[-1]

            def update_weights(model, X, y_true):
                with tf.GradientTape() as tape:
                    y_pred = model(X)
                    loss = tf.keras.losses.mean_squared_error(y_true, y_pred)
                gradients = tape.gradient(loss, model.trainable_variables)
                optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
                optimizer.apply_gradients(zip(gradients, model.trainable_variables))

            last_data = (
                data[["open", "close", "high", "low", "volume", "daily_return"]]
                .iloc[-1]
                .values
            )
            last_data = last_data.reshape((1, 1, last_data.shape[0]))
            predicted_next_day_close = model.predict(last_data)
            print(f"Predicted Next Day Close: {predicted_next_day_close[0][0]: .4f}")
            diff = (predicted_next_day_close[0][0] - today_close) / today_close * 100
            if predicted_next_day_close[0][0] > today_close:
                data_l.append([symbol, predicted_next_day_close[0][0], "up", diff])
            else:
                data_l.append([symbol, predicted_next_day_close[0][0], "down", diff])
        except Exception as e:
            print(e)

    symbols = [
        "AAPL",
        "MSFT",
        "GOOGL",
        "AMZN",
        "BRKA",
        "NVDA",
        "META",
        "TSLA",
        "V",
        "UNH",
    ]

    for i in symbols:
        predict_tomorrow_stock_price(i)

    df = pd.DataFrame(data_l, columns=new_columns)
    df = df.reset_index(drop=True)
    df.index = df.index + 1

    df.to_sql(
        "ml_snp_stock_tomorrow_close", engine, schema="analytics", if_exists="replace"
    )
    return


@task
def nas_ml():
    task_logger.info("nas_ml")
    new_columns = ["symbol", "tomorrow_close", "up/down", "diff"]
    df = pd.DataFrame(columns=new_columns)
    data_l = []

    def predict_tomorrow_stock_price(symbol):
        try:
            data_query = f"SELECT date, open, close, high, low, volume FROM analytics.nas_stock_{symbol}"

            data = load_data_from_db(data_query)

            data["date"] = pd.to_datetime(data["date"])
            data = data.sort_values("date")

            data["daily_return"] = data["close"].pct_change()
            data.dropna(inplace=True)
            scaler = StandardScaler()
            data["volume"] = scaler.fit_transform(data[["volume"]])

            def build_recursive_model(input_shape):
                model = tf.keras.Sequential(
                    [
                        tf.keras.layers.LSTM(
                            50, activation="relu", input_shape=input_shape
                        ),
                        tf.keras.layers.Dense(1),
                    ]
                )
                model.compile(optimizer="adam", loss="mean_squared_error")
                return model

            X = data[["open", "close", "high", "low", "volume", "daily_return"]].values[
                :-1
            ]
            y = data["close"].values[1:]
            X = X.reshape((X.shape[0], 1, X.shape[1]))

            model = build_recursive_model((X.shape[1], X.shape[2]))

            def lr_schedule(epoch, lr):
                if epoch < 10:
                    return lr
                else:
                    return lr * tf.math.exp(-0.1)

            lr_scheduler = LearningRateScheduler(lr_schedule)
            model.fit(
                X, y, epochs=20, batch_size=16, verbose=1, callbacks=[lr_scheduler]
            )

            today_close = data["close"].iloc[-1]

            def update_weights(model, X, y_true):
                with tf.GradientTape() as tape:
                    y_pred = model(X)
                    loss = tf.keras.losses.mean_squared_error(y_true, y_pred)
                gradients = tape.gradient(loss, model.trainable_variables)
                optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
                optimizer.apply_gradients(zip(gradients, model.trainable_variables))

            last_data = (
                data[["open", "close", "high", "low", "volume", "daily_return"]]
                .iloc[-1]
                .values
            )
            last_data = last_data.reshape((1, 1, last_data.shape[0]))
            predicted_next_day_close = model.predict(last_data)
            print(f"Predicted Next Day Close: {predicted_next_day_close[0][0]: .4f}")
            diff = (predicted_next_day_close[0][0] - today_close) / today_close * 100
            if predicted_next_day_close[0][0] > today_close:
                data_l.append([symbol, predicted_next_day_close[0][0], "up", diff])
            else:
                data_l.append([symbol, predicted_next_day_close[0][0], "down", diff])
        except Exception as e:
            print(e)

    symbols = [
        "aapl",
        "msft",
        "googl",
        "amzn",
        "nvda",
        "meta",
        "tsla",
        "pep",
        "avgo",
        "asml",
    ]

    for i in symbols:
        predict_tomorrow_stock_price(i)

    df = pd.DataFrame(data_l, columns=new_columns)
    df = df.reset_index(drop=True)
    df.index = df.index + 1

    df.to_sql(
        "ml_nas_stock_tomorrow_close", engine, schema="analytics", if_exists="replace"
    )
    return


@task
def krx_ml():
    task_logger.info("krx_ml")
    new_columns = ["name", "tomorrow_close", "up/down", "diff"]
    df = pd.DataFrame(columns=new_columns)
    data_l = []

    def predict_tomorrow_stock_price(code, name):
        try:
            data_query = f"SELECT date, open, close, high, low, volume FROM analytics.krx_stock_{code}"
            data = load_data_from_db(data_query)

            data["date"] = pd.to_datetime(data["date"])
            data = data.sort_values("date")

            data["daily_return"] = data["close"].pct_change()
            data.dropna(inplace=True)
            scaler = StandardScaler()
            data["volume"] = scaler.fit_transform(data[["volume"]])

            def build_recursive_model(input_shape):
                model = tf.keras.Sequential(
                    [
                        tf.keras.layers.LSTM(
                            50, activation="relu", input_shape=input_shape
                        ),
                        tf.keras.layers.Dense(1),
                    ]
                )
                model.compile(optimizer="adam", loss="mean_squared_error")
                return model

            X = data[["open", "close", "high", "low", "volume", "daily_return"]].values[
                :-1
            ]
            y = data["close"].values[1:]
            X = X.reshape((X.shape[0], 1, X.shape[1]))

            model = build_recursive_model((X.shape[1], X.shape[2]))

            def lr_schedule(epoch, lr):
                if epoch < 10:
                    return lr
                else:
                    return lr * tf.math.exp(-0.1)

            lr_scheduler = LearningRateScheduler(lr_schedule)
            model.fit(
                X, y, epochs=20, batch_size=16, verbose=1, callbacks=[lr_scheduler]
            )

            today_close = data["close"].iloc[-1]

            def update_weights(model, X, y_true):
                with tf.GradientTape() as tape:
                    y_pred = model(X)
                    loss = tf.keras.losses.mean_squared_error(y_true, y_pred)
                gradients = tape.gradient(loss, model.trainable_variables)
                optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
                optimizer.apply_gradients(zip(gradients, model.trainable_variables))

            last_data = (
                data[["open", "close", "high", "low", "volume", "daily_return"]]
                .iloc[-1]
                .values
            )
            last_data = last_data.reshape((1, 1, last_data.shape[0]))
            predicted_next_day_close = model.predict(last_data)
            print(f"Predicted Next Day Close: {predicted_next_day_close[0][0]: .4f}")
            diff = (predicted_next_day_close[0][0] - today_close) / today_close * 100
            if predicted_next_day_close[0][0] > today_close:
                data_l.append([name, predicted_next_day_close[0][0], "up", diff])
            else:
                data_l.append([name, predicted_next_day_close[0][0], "down", diff])
        except Exception as e:
            print(e)

    codes = [
        ("005930", "삼성전자"),
        ("005935", "삼성전자우"),
        ("373220", "LG엔솔"),
        ("000660", "SK하이닉스"),
        ("207940", "삼성바이오로직스"),
        ("051910", "LG화학"),
        ("051915", "LG화학우"),
        ("006400", "삼성SDI"),
        ("006405", "삼성SDI우"),
        ("005389", "현대차3우B"),
    ]

    for i in codes:
        predict_tomorrow_stock_price(i[0], i[1])

    df = pd.DataFrame(data_l, columns=new_columns)
    df = df.reset_index(drop=True)
    df.index = df.index + 1
    print(df)

    df.to_sql(
        "ml_krx_stock_tomorrow_close", engine, schema="analytics", if_exists="replace"
    )

    return


with DAG(
    dag_id="ml_dag",
    schedule="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    (snp_ml() >> nas_ml() >> krx_ml())
