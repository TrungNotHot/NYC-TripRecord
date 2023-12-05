from typing import Union
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.parquet as pq


def make_bucket(client: Minio, bucket_name):
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket {bucket_name} already exists.")


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.minio_client = Minio(
            self._config["endpoint_url"],
            access_key=self._config["aws_access_key_id"],
            secret_key=self._config["aws_secret_access_key"],
            secure=False
        )

    def _get_path(self, context: Union[OutputContext, InputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file_{}.parquet".format(
            "_".join(context.asset_key.path),
        )
        if context.has_partition_key:
            partition_str = str(table) + "_" + context.asset_partition_key
            return os.path.join(key, f"{partition_str}.parquet"), tmp_file_path
        else:
            return f"{key}.parquet", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        pass

    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key_name, tmp_file_path = self._get_path(context)
        table = pa.Table.from_pandas(obj)
        pq.write_table(table, tmp_file_path)

        try:
            bucket_name = self._config["bucket"]
            make_bucket(self.minio_client, bucket_name)
            self.minio_client.fput_object(
                bucket_name, key_name, tmp_file_path,
            )
            context.log.info(
                f"(MinIO handle_output) Number of rows and columns: {obj.shape}"
            )
            # Clean up tmp file
            os.remove(tmp_file_path)
        except Exception as e:
            raise e

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        bucket_name = self._config["bucket"]
        key_name, tmp_file_path = self._get_path(context)
        try:
            context.log.info(f"(MinIO load_input) from key_name: {key_name}")
            self.minio_client.fget_object(
                bucket_name, key_name, tmp_file_path,
            )
            df = pd.read_parquet(tmp_file_path)
            os.remove(tmp_file_path)
            return df
        except Exception as e:
            raise e
