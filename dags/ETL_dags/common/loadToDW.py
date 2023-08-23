from sqlalchemy import create_engine, text


class LoadToDW:
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

    def drop_table(self, schema, table):
        self.engine.execute(text(f"DROP TABLE IF EXISTS {schema}.{table};"))

    def create_table(self, schema, table, column_type, primary_key):
        query = f"CREATE TABLE {schema}.{table}("
        for column, type in column_type.items():
            query += f"{column} {type},"

        query += f"CONSTRAINT PK_{table} PRIMARY KEY({primary_key}));"
        self.engine.execute(text(query))

        # self.engine.execute(
        #     text(
        #         f"""
        #             CREATE TABLE {schema}.{table}(
        #             {columns[0]} VARCHAR(40),
        #             ISU_CD VARCHAR(40),
        #             Name VARCHAR(40),
        #             Market VARCHAR(40),
        #             Dept VARCHAR(40),
        #             Close VARCHAR(40),
        #             ChangeCode VARCHAR(40),
        #             Changes VARCHAR(40),
        #             ChangesRatio VARCHAR(40),
        #             Open VARCHAR(40),
        #             High VARCHAR(40),
        #             Low VARCHAR(40),
        #             Volume VARCHAR(40),
        #             Amount VARCHAR(40),
        #             Marcap VARCHAR(40),
        #             Stocks VARCHAR(40),
        #             MarketId VARCHAR(40),
        #             CONSTRAINT PK_{table} PRIMARY KEY({primary_key})
        #         );"""
        #     )
        # )

    def table_import_from_s3(
        self,
        schema,
        table,
        s3_bucket,
        s3_object,
        region,
        aws_access_key_id,
        aws_secret_access_key,
    ):
        query = f"""
            SELECT aws_s3.table_import_from_s3(
            '{schema}.{table}', '', '(format csv)',
            aws_commons.create_s3_uri('{s3_bucket}', '{s3_object}', '{region}'),
            aws_commons.create_aws_credentials('{aws_access_key_id}', '{aws_secret_access_key}', '')
            );
        """
        self.engine.execute(text(query))

    #         engine.execute(
    #     text(
    #         f"""
    #         SELECT aws_s3.table_import_from_s3(
    #         'raw_data.krx_list', '', '(format csv)',
    #         aws_commons.create_s3_uri('de-5-2', 'krx_list.csv', 'ap-northeast-2'),
    #         aws_commons.create_aws_credentials('{CONFIG["AWS_ACCESS_KEY_ID"]}', '{CONFIG["AWS_SECRET_ACCESS_KEY"]}', '')
    #     );"""
    #     )
    # )

    def delete_wrong_row(self, schema, table, clause):
        query = f"DELETE FROM {schema}.{table} WHERE {clause};"
        self.engine.execute(text(query))
        # engine.execute(text("DELETE FROM raw_data.krx_list WHERE code like '%Code%';"))

    def alter_column_type(self, schema, table, column_type):
        query = f"ALTER TABLE {schema}.{table} "
        for column, type in column_type.items():
            query += f"ALTER COLUMN {column} TYPE {type} USING {column}::{type},"
        query = query[:-1] + ";"
        self.engine.execute(text(query))
        # engine.execute(

    #     text(
    #         """
    #             ALTER TABLE raw_data.krx_list
    #                 ALTER COLUMN Close TYPE INTEGER USING Close::INTEGER,
    #                 ALTER COLUMN ChangeCode TYPE INTEGER USING ChangeCode::INTEGER,
    #                 ALTER COLUMN Changes TYPE INTEGER USING Changes::INTEGER,
    #                 ALTER COLUMN ChangesRatio TYPE FLOAT USING ChangesRatio::FLOAT,
    #                 ALTER COLUMN Open TYPE INTEGER USING Open::INTEGER,
    #                 ALTER COLUMN High TYPE INTEGER USING High::INTEGER,
    #                 ALTER COLUMN Low TYPE INTEGER USING Low::INTEGER,
    #                 ALTER COLUMN Volume TYPE INTEGER USING Volume::INTEGER,
    #                 ALTER COLUMN Amount TYPE BIGINT USING Amount::BIGINT,
    #                 ALTER COLUMN Marcap TYPE BIGINT USING Marcap::BIGINT,
    #                 ALTER COLUMN Stocks TYPE BIGINT USING Stocks::BIGINT;
    #             """
    #     )
    # )

    def close_connection(self):
        self.conn.close()

    def dispose_sqlalchemy_engine(self):
        self.engine.dispose()
