from sqlalchemy import create_engine, text


class DB:
    def __init__(self, dw_user, dw_password, dw_host, dw_port, dw_dbname):
        self.connection_uri = "postgresql://{}:{}@{}:{}/{}".format(
            dw_user,
            dw_password,
            dw_host,
            dw_port,
            dw_dbname,
        )

    def create_sqlalchemy_engine(self):
        self.engine = create_engine(
            self.connection_uri, pool_pre_ping=True, isolation_level="AUTOCOMMIT"
        )

    def connect_engine(self):
        self.conn = self.engine.connect()

    def close_connection(self):
        self.conn.close()

    def dispose_sqlalchemy_engine(self):
        self.engine.dispose()
