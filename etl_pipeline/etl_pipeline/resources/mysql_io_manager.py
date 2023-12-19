from dagster import IOManager, InputContext, OutputContext
import polars as pl


def connect_mysql(config) -> str:
    conn = (
        f"mysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    return conn


class MySQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: "OutputContext", obj: pl.DataFrame):
        pass

    def load_input(self, context: "InputContext"):
        pass

    def extract_data(self, sql: str) -> pl.DataFrame:
        conn = connect_mysql(self._config)
        df_data = pl.read_database(query=sql, connection_uri=conn)
        return df_data