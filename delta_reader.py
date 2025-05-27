from pyspark.sql import DataFrame
from libs.readers.base_reader import BaseReader
from libs.error_handler import ETLErrorHandler

class DeltaReader(BaseReader):
    @ETLErrorHandler.handle_spark_errors()
    def read(self) -> DataFrame:
        """Read batch data from Delta table"""
        table_path = self._get_table_path()
        self.logger.info(f"Reading batch data from Delta table: {table_path}")
        
        df = self.spark.read \
            .format("delta") \
            .option("versionAsOf", self.config.get("version")) \
            .load(table_path)
            
        self.logger.info(f"Successfully read {df.count()} rows from {table_path}")
        return df

    @ETLErrorHandler.handle_spark_errors()
    def read_stream(self) -> DataFrame:
        """Read streaming data from Delta table with Change Data Feed"""
        table_path = self._get_table_path()
        self.logger.info(f"Starting stream read from Delta table: {table_path}")
        
        df = self.spark.readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", self.config.get("startingVersion", "latest")) \
            .load(table_path)
            
        self.logger.info(f"Successfully started streaming from {table_path}")
        return df

    def _get_table_path(self) -> str:
        """Construct full table path from catalog, schema, and table name"""
        catalog = self.config.get("catalog", "hive_metastore")
        schema = self.config.get("schema", "default")
        table = self.config["table"]
        return f"{catalog}.{schema}.{table}"