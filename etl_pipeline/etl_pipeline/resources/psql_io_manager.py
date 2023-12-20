from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import DataFrame
import polars as pl



def connect_psql(config) -> str:
    conn = (
        f"postgresql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    return conn


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config


    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        table = context.asset_key.path[-1]
        schema_ = context.asset_key.path[-2]
        conn = connect_psql(self._config)
        obj.write_database(table, conn, if_exists="replace", engine='sqlalchemy')

        context.log.info(f"Uploaded {context.asset_key} to PostgreSQL")

    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass

# def connect_psql(config, table):
#     conn = {
#         "url": f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}",
#         "dbtable": table,
#         "user": config["user"],
#         "password": config["password"],
#     }
#     return conn

# class PostgreSQLIOManager(IOManager):
#     def __init__(self, config):
#         self._config = config

#     def handle_output(self, context: OutputContext, obj: DataFrame):
#         table = context.asset_key.path[-1]
#         schema_ = context.asset_key.path[-2]
#         full_table = f"{schema_}.{table}"
#         conn = connect_psql(self._config, full_table)
#         obj.write \
#             .format("jdbc") \
#             .option("url", conn['url'] ) \
#             .option("dbtable", conn['dbtable']) \
#             .option("user", conn['user']) \
#             .option("password", conn['password']) \
#             .save()


#     def load_input(self, context: InputContext) -> pl.DataFrame:
#         pass