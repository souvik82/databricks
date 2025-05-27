from pyspark.sql import DataFrame
from libs.readers.base_reader import BaseReader
from libs.error_handler import ETLErrorHandler
from databricks.sdk import WorkspaceClient

class DeltaReader(BaseReader):
    def __init__(self, config: Dict[str, Any], logger: ETLLogger = None):
        super().__init__(config, logger)
        self.uc_client = WorkspaceClient()
    
    def _get_uc_table_path(self) -> str:
        """Get fully qualified Unity Catalog table path"""
        catalog = self.config.get('catalog', 'hive_metastore')
        schema = self.config.get('schema', 'default')
        table = self.config['table']
        return f"{catalog}.{schema}.{table}"
    
    @ETLErrorHandler.handle_spark_errors()
    def read(self) -> DataFrame:
        """Read batch data from Delta table with UC support"""
        table_path = self._get_uc_table_path()
        self.logger.info(f"Reading batch data from Delta table: {table_path}")
        
        reader = self.spark.read.format("delta")
        
        if 'version' in self.config:
            reader = reader.option("versionAsOf", self.config['version'])
        
        df = reader.table(table_path)
        self.logger.info(f"Successfully read {df.count()} rows from {table_path}")
        return df
    
    @ETLErrorHandler.handle_spark_errors()
    def read_stream(self) -> DataFrame:
        """Read streaming data from Delta table with Change Data Feed"""
        table_path = self._get_uc_table_path()
        self.logger.info(f"Starting stream read from Delta table: {table_path}")
        
        reader = self.spark.readStream \
            .format("delta") \
            .option("readChangeFeed", "true")
        
        if 'startingVersion' in self.config:
            reader = reader.option("startingVersion", self.config['startingVersion'])
        
        df = reader.table(table_path)
        self.logger.info(f"Successfully started streaming from {table_path}")
        return df