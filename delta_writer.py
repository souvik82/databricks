from pyspark.sql import DataFrame
from libs.writers.base_writer import BaseWriter
from libs.error_handler import ETLErrorHandler

class DeltaWriter(BaseWriter):
    @ETLErrorHandler.handle_spark_errors()
    def write(self, df: DataFrame) -> None:
        """Write batch data to Delta table"""
        table_path = self._get_table_path()
        self.logger.info(f"Starting batch write to Delta table: {table_path}")
        
        writer = df.write.format("delta")
        
        # Apply partitioning if configured
        if "partition_columns" in self.config:
            writer = writer.partitionBy(*self.config["partition_columns"])
            self.logger.info(f"Using partition columns: {self.config['partition_columns']}")
        
        # Apply write mode
        mode = self.config.get("mode", "append")
        if mode == "overwrite":
            writer = writer.mode("overwrite")
        elif mode == "append":
            writer = writer.mode("append")
        elif mode == "ignore":
            writer = writer.mode("ignore")
        elif mode == "error":
            writer = writer.mode("error")
        elif mode == "errorifexists":
            writer = writer.mode("errorifexists")
        
        # Save the data
        writer.saveAsTable(table_path)
        self.logger.info(f"Successfully wrote batch data to {table_path}")

    @ETLErrorHandler.handle_spark_errors()
    def write_stream(self, df: DataFrame) -> None:
        """Write streaming data to Delta table"""
        table_path = self._get_table_path()
        checkpoint_location = self.config.get("checkpoint_location")
        
        if not checkpoint_location:
            raise ValueError("checkpoint_location is required for streaming writes")
        
        self.logger.info(f"Starting stream write to Delta table: {table_path}")
        
        writer = df.writeStream \
            .format("delta") \
            .option("checkpointLocation", checkpoint_location)
        
        # Apply partitioning if configured
        if "partition_columns" in self.config:
            writer = writer.partitionBy(*self.config["partition_columns"])
            self.logger.info(f"Using partition columns: {self.config['partition_columns']}")
        
        # Start the stream
        query = writer.toTable(table_path)
        
        self.logger.info(f"Started streaming write to {table_path} with query ID: {query.id}")
        return query

    def _get_table_path(self) -> str:
        """Construct full table path from catalog, schema, and table name"""
        catalog = self.config.get("catalog", "hive_metastore")
        schema = self.config.get("schema", "default")
        table = self.config["table"]
        return f"{catalog}.{schema}.{table}"