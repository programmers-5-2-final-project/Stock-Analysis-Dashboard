from sqlalchemy import create_engine, text


class LoadToRDS:
    def __init__(self, conn):
        self.conn = conn

    def drop_table(self, schema, table):
        self.conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table};"))

    def create_table(self, schema, table, column_type, primary_key=None):
        query = f"CREATE TABLE {schema}.{table}("
        for column, type in column_type.items():
            query += f"{column} {type},"

        if primary_key:
            query += f" PRIMARY KEY({primary_key}));"
        else:
            query = query[:-1] + ");"
        self.conn.execute(text(query))

    def install_aws_s3_extension(self):
        self.conn.execute(text("CREATE EXTENSION aws_s3 CASCADE;"))

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
        result = self.conn.execute(text(query))
        print(result.fetchall())

    def table_import_from_local(self, schema, table):
        # query = f"""
        #     COPY {schema}.{table} FROM 'data/{schema}.csv' DELIMITER ',' CSV HEADER;
        # """
        query = f"""
            LOAD DATA LOCAL INFILE 'data/{table}.csv' 
            INTO TABLE {schema}.{table} 
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n' 
            IGNORE 1 ROWS;
        """
        result = self.conn.execute(text(query))
        print(result.fetchall())

    def delete_wrong_row(self, schema, table, clause):
        query = f"DELETE FROM {schema}.{table} WHERE {clause};"
        self.conn.execute(text(query))

    def alter_column_type(self, schema, table, column_type):
        # query = f"ALTER TABLE {schema}.{table} "
        # for column, type in column_type.items():
        #     query += f"ALTER COLUMN {column} TYPE {type} USING {column}::{type},"
        # query = query[:-1] + ";"
        # self.conn.execute(text(query))
        for column, type in column_type.items():
            query = f"ALTER TABLE {schema}.{table} MODIFY {column} {type};"
            self.conn.execute(text(query))


# class LoadToRedshift:
#     def __init__(self, conn):
#         self.conn = conn

#     def drop_table(self, schema, table):
#         self.conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table};"))

#     def create_table(self, schema, table, column_type, primary_key=None):
#         query = f"CREATE TABLE {schema}.{table}("
#         for column, type in column_type.items():
#             query += f'"{column}" {type},'

#         if primary_key:
#             query += f" PRIMARY KEY({primary_key}));"
#         else:
#             query = query[:-1] + ");"
#         self.conn.execute(text(query))

#     def table_import_from_s3(
#         self,
#         schema,
#         table,
#         s3_bucket,
#         s3_object,
#     ):
#         query = f"""
#             COPY dev.{schema}.{table} FROM 's3://{s3_bucket}/{s3_object}' IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20230826T161520' FORMAT AS CSV DELIMITER ',' QUOTE '"' REGION AS 'ap-northeast-2';
#         """
#         self.conn.execute(text(query).execution_options(autocommit=True))

#     def delete_wrong_row(self, schema, table, clause):
#         query = f"DELETE FROM {schema}.{table} WHERE {clause};"
#         self.conn.execute(text(query).execution_options(autocommit=True))

#     def alter_column_type(self, schema, table, column_type):
#         query = f"ALTER TABLE {schema}.{table} "
#         for column, type in column_type.items():
#             query += f'ALTER COLUMN "{column}" TYPE {type} USING "{column}"::{type},'
#         query = query[:-1] + ";"
#         self.conn.execute(text(query))
