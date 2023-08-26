from sqlalchemy import create_engine, text


class LoadToDW:
    def __init__(self, engine):
        self.engine = engine

    def drop_table(self, schema, table):
        self.engine.execute(text(f"DROP TABLE IF EXISTS {schema}.{table};"))

    def create_table(self, schema, table, column_type, primary_key=None):
        query = f"CREATE TABLE {schema}.{table}("
        for column, type in column_type.items():
            query += f"{column} {type},"

        if primary_key:
            query += f" PRIMARY KEY({primary_key}));"
        else:
            query = query[:-1] + ");"
        self.engine.execute(text(query))

    def install_aws_s3_extension(self):
        self.engine.execute(text("CREATE EXTENSION aws_s3 CASCADE;"))

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

    def delete_wrong_row(self, schema, table, clause):
        query = f"DELETE FROM {schema}.{table} WHERE {clause};"
        self.engine.execute(text(query))

    def alter_column_type(self, schema, table, column_type):
        query = f"ALTER TABLE {schema}.{table} "
        for column, type in column_type.items():
            query += f"ALTER COLUMN {column} TYPE {type} USING {column}::{type},"
        query = query[:-1] + ";"
        self.engine.execute(text(query))
