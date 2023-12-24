from dagster import IOManager, OutputContext, InputContext
from pyspark.sql import SparkSession, DataFrame
from contextlib import contextmanager



@contextmanager
def get_spark_session(config, run_id="Spark IO Manager"):
    executor_memory = "1g" if run_id != "Spark IO Manager" else "1500m"
    try:
        spark = (
            # SparkSession.builder.master("spark://spark-master:7077")
            SparkSession.builder.master("local[*]")
            .appName(run_id)
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", executor_memory)
            .config("spark.cores.max", "4")
            .config("spark.executor.cores", "2")
            .config(
                "spark.jars",
                "/usr/local/spark/jars/delta-core_2.12-2.2.0.jar,/usr/local/spark/jars/hadoop-aws-3.3.2.jar,/usr/local/spark/jars/delta-storage-2.2.0.jar,/usr/local/spark/jars/aws-java-sdk-1.12.367.jar,/usr/local/spark/jars/s3-2.18.41.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
            )
            .config("spark.driver.extraClassPath", "/usr/local/spark/jars/postgresql-42.7.1.jar")
            .config("spark.executor.extraClassPath", "/usr/local/spark/jars/postgresql-42.7.1.jar")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{config['endpoint_url']}")
            .config("spark.hadoop.fs.s3a.access.key", str(config["minio_access_key"]))
            .config("spark.hadoop.fs.s3a.secret.key", str(config["minio_secret_key"]))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            .getOrCreate()
        )
        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")
    
class SparkIOManager(IOManager):
    def __init__(self, config):
        self._config = config
    
    def handle_output(self, context: OutputContext, obj: DataFrame):
        # Write output to s3a (MinIO)
        context.log.debug("(Spark handle_output) Writing output to MinIO ...")

        file_path = "s3a://lakehouse/" + "/".join(context.asset_key.path)
        if context.has_partition_key:
            file_path += f"/{context.partition_key}"
        context.log.debug(f"(Spark handle_output) File path: {file_path}")
        file_name = str(context.asset_key.path[-1])
        context.log.debug(f"(Spark handle_output) File name: {file_name}")
        
        try:
            obj.write.mode("overwrite").parquet(file_path)
            context.log.debug(f"Saved {file_name} to {file_path}")
        except Exception as e:
            raise Exception(f"(Spark handle_output) Error while writing output: {e}")
    
    def load_input(self, context: InputContext) -> DataFrame:
        context.log.debug(f"Loading input from {context.asset_key.path}...")
        file_path = "s3a://lakehouse/" + "/".join(context.asset_key.path)
        check_partition = (context.metadata or {}).get("partition", True) 
        if check_partition == True:  
            if context.has_partition_key:
                file_path += f"/{context.partition_key}"
        full_load = (context.metadata or {}).get("full_load", False)

        try:
            with get_spark_session(self._config) as spark:
                df = None
                if full_load:
                    df = (
                        spark.read.format("parquet")
                        .options(header=True, inferSchema=False)
                        .load(file_path + "/*")
                    )
                else:
                    df = spark.read.parquet(file_path)

                return df
        except Exception as e:
            raise Exception(f"Error while loading input: {e}")