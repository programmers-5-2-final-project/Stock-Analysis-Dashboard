from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


class DB:
    def __init__(self, dw_user, dw_password, dw_host, dw_port, dw_dbname):
        # MySQL 연결 문자열로 변경
        self.connection_uri = "mysql+pymysql://{}:{}@{}:{}/{}".format(
            dw_user,
            dw_password,
            dw_host,
            dw_port,
            dw_dbname,
        )
        self.engine = None
        self.connect()

    def connect(self):
        try:
            # 엔진을 생성하고 데이터베이스에 연결합니다.
            self.engine = create_engine(
                self.connection_uri, echo=True
            )  # echo=True는 로깅을 활성화합니다.
            # 연결 테스트를 수행합니다.
            self.engine.execute("SELECT 1")
            print("데이터베이스에 성공적으로 연결되었습니다.")
        except SQLAlchemyError as e:
            print("데이터베이스 연결에 실패하였습니다.")
            print(str(e))

    def create_sqlalchemy_engine(self):
        self.engine = create_engine(self.connection_uri, pool_pre_ping=True)

    def connect_engine(self):
        self.conn = self.engine.connect()

    def close_connection(self):
        self.conn.close()

    def dispose_sqlalchemy_engine(self):
        self.engine.dispose()
