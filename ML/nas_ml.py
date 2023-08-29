import pandas as pd
import numpy as np
import sqlalchemy
import tensorflow as tf
import os
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
from dotenv import dotenv_values
from sqlalchemy import text


CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ

# PostgreSQL RDS 연결 정보 설정
db_host = CONFIG["POSTGRES_HOST"]
db_port = CONFIG["POSTGRES_PORT"]
db_name = "dev"
db_user = CONFIG["POSTGRES_USER"]
db_password = CONFIG["POSTGRES_PASSWORD"]

# SQLAlchemy 연결 URL 생성
db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = sqlalchemy.create_engine(db_url)


def load_data_from_db(query):
    conn = engine.connect()
    data = pd.read_sql(query, conn)
    conn.close()
    return data


new_columns = ["symbol", "tomorrow_close", "mse"]
df = pd.DataFrame(columns=new_columns)
data_l = []


def predict_tomorrow_stock_price(symbol):
    try:
        data_query = f"SELECT date, open, close, high, low, volume FROM analytics.nas_stock_{symbol}"  # 'date' 및 추가 컬럼 포함

        # 데이터 불러오기
        data = load_data_from_db(data_query)

        # Sort the data by date in ascending order
        data["date"] = pd.to_datetime(data["date"])
        data = data.sort_values("date")
        # print(data.head())

        # Calculate daily returns
        data["daily_return"] = data["close"].pct_change()
        data.dropna(inplace=True)
        scaler = StandardScaler()
        data["volume"] = scaler.fit_transform(data[["volume"]])
        # print(data.head())

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

        # 모델 학습
        X = data[["open", "close", "high", "low", "volume", "daily_return"]].values[1:]
        y = data["close"].values[1:]  # Use the current day's close values
        # print("y", y)
        X = X.reshape((X.shape[0], 1, X.shape[1]))

        model = build_recursive_model((X.shape[1], X.shape[2]))
        model.fit(X, y, epochs=20, batch_size=16, verbose=1)

        # Calculate predictions
        y_pred = model.predict(X)[:-1]

        # Calculate mean squared error
        mse = mean_squared_error(y[:-1], y_pred)
        # print(f"Mean Squared Error: {mse}")
        # print(f"True Close Values: {y[:-1]}")

        # Update model weights using gradient descent (not shown here)
        def update_weights(model, X, y_true):
            with tf.GradientTape() as tape:
                y_pred = model(X)
                loss = tf.keras.losses.mean_squared_error(y_true, y_pred)
                # print(loss)
            gradients = tape.gradient(loss, model.trainable_variables)
            optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
            optimizer.apply_gradients(zip(gradients, model.trainable_variables))

        last_data = (
            data[["open", "close", "high", "low", "volume", "daily_return"]]
            .iloc[-1, :]
            .values
        )
        last_data = last_data.reshape((1, 1, last_data.shape[0]))
        predicted_next_day_close = model.predict(last_data)
        print(f"Predicted Next Day Close: {predicted_next_day_close[0][0]: .4f}")
        data_l.append([symbol, predicted_next_day_close[0][0], mse])
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
df = df.reset_index(drop=True)  # 기존 인덱스 삭제하고 숫자 인덱스로 재설정
df.index = df.index + 1  # 인덱스를 1부터 시작하도록 조정
print(df)


table_name = "ml_nas_stock_tomorrow_close"
create_table_query = f"""
    DROP TABLE IF EXISTS analytics.{table_name};
    CREATE TABLE analytics.{table_name}(
        Symbol VARCHAR(40),
        Tomorrow_Close VARCHAR(40),
        MSE VARCHAR(40)
    );"""


with engine.connect() as conn:
    conn.execute(text(create_table_query))
    df.to_sql(table_name, conn, schema="analytics", if_exists="replace")