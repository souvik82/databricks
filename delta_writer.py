from pyspark.sql import DataFrame
from libs.writers.base_writer import BaseWriter
from libs.error_handler import ETLErrorHandler
from databricks.sdk import WorkspaceClient

class DeltaWriter(BaseWriter):
    def __init__(self, config: Dict[str, Any], logger: ETLLogger = None):
        super().__init__(config, logger)
        self.uc_client = WorkspaceClient()
    
    def _get_uc_table_path(self) -> str:
        """Get fully qualified Unity Catalog table path"""
        catalog = self.config.get('catalog', 'hive_metastore')
        schema = self.config.get('schema', 'default')
        table = self.config['table']
        return f"{catalog}.{schema}.{table}"
    
    def _create_uc_table_if_not_exists(self, df: DataFrame):
        """Create UC table if it doesn't exist"""
        from delta.tables import DeltaTable
        
        table_path = self._get_uc_table_path()
        catalog = self.config.get('catalog', 'hive_metastore')
        
        if catalog != 'hive_metastore' and not DeltaTable.isDeltaTable(self.spark, table_path):
            self.logger.info(f"Creating Unity Catalog table: {table_path}")
            
            # Create table using UC API
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_path} (
                    {self._generate_schema_ddl(df.schema)}
                )
                USING DELTA
                {self._generate_partition_ddl()}
                {self._generate_table_properties()}
            """)
    
    def _generate_schema_ddl(self, schema) -> str:
        """Generate DDL for schema"""
        from pyspark.sql.types import StructType
        
        if isinstance(schema, StructType):
            return ", ".join([f"{field.name} {field.dataType.simpleString()}" 
                            for field in schema.fields])
        return ""
    
    def _generate_partition_ddl(self) -> str:
        """Generate PARTITIONED BY clause if configured"""
        if 'partition_columns' in self.config:
            return f"PARTITIONED BY ({', '.join(self.config['partition_columns'])})"
        return ""
    
    def _generate_table_properties(self) -> str:
        """Generate TBLPROPERTIES clause"""
        props = []
        if self.config.get('schema_evolution', False):
            props.append("'delta.enableSchemaEvolution' = 'true'")
        if props:
            return f"TBLPROPERTIES ({', '.join(props)})"
        return ""
    
    @ETLErrorHandler.handle_spark_errors()
    def write(self, df: DataFrame) -> None:
        """Write batch data to Delta table with UC support"""
        table_path = self._get_uc_table_path()
        
        # Create UC table if needed
        self._create_uc_table_if_not_exists(df)
        
        writer = df.write.format("delta")
        
        # Apply write mode
        mode = self.config.get('mode', 'append')
        writer = writer.mode(mode)
        
        # Apply partitioning if configured
        if 'partition_columns' in self.config:
            writer = writer.partitionBy(*self.config['partition_columns'])
            self.logger.info(f"Using partition columns: {self.config['partition_columns']}")
        
        # Save the data
        writer.saveAsTable(table_path)
        self.logger.info(f"Successfully wrote to Delta table: {table_path}")
        
        # Run optimization if configured
        if self.config.get('optimize_settings', {}).get('auto_optimize', False):
            self._optimize_table(table_path)
    
    def _optimize_table(self, table_path: str):
        """Optimize Delta table with UC support"""
        zorder_cols = self.config['optimize_settings'].get('zorder_columns', [])
        
        if zorder_cols:
            self.spark.sql(f"OPTIMIZE {table_path} ZORDER BY ({', '.join(zorder_cols)})")
        else:
            self.spark.sql(f"OPTIMIZE {table_path}")
        
        if self.config.get('optimize_settings', {}).get('vacuum', {}).get('enabled', False):
            retain_hours = self.config['optimize_settings']['vacuum'].get('retain_hours', 168)
            self.spark.sql(f"VACUUM {table_path} RETAIN {retain_hours} HOURS")