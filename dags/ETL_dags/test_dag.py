from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# MYSQL 설정
MYSQL_DB = "raw_data"
MYSQL_USER = "myuser"
MYSQL_PASSWORD = "mypassword"
MYSQL_HOST = "0.0.0.0"
MYSQL_PORT = 3306


def connect():
    try:
        connection_uri = "mysql+pymysql://{}:{}@{}:{}/{}".format(
            MYSQL_USER,
            MYSQL_PASSWORD,
            MYSQL_HOST,
            MYSQL_PORT,
            MYSQL_DB,
        )
        # 엔진을 생성하고 데이터베이스에 연결합니다.
        engine = create_engine(connection_uri, echo=True)  # echo=True는 로깅을 활성화합니다.
        # 연결 테스트를 수행합니다.
        engine.execute("SELECT 1")
        # 데이터베이스 목록을 조회합니다.
        result = engine.execute("SHOW SCHEMAS;")
        print("데이터베이스 목록:")
        for row in result:
            print(" -", row[0])
        print("데이터베이스에 성공적으로 연결되었습니다.")
    except SQLAlchemyError as e:
        print("데이터베이스 연결에 실패하였습니다.")
        print(str(e))


connect()
