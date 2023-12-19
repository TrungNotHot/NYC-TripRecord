from dagster import IOManager, InputContext, OutputContext
from contextlib import contextmanager
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
        schema = context.asset_key.path[-2]
        conn = connect_psql(self._config)
        obj.write_database(table, conn, if_exists="replace", engine = 'sqlalchemy')

        print("Write successfully!")

    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass