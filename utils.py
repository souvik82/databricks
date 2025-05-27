import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from typing import Dict, Any
import os

class ConfigLoader:
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """Load YAML configuration file"""
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    
    @staticmethod
    def get_environment_config(env: str = "dev") -> Dict[str, Any]:
        """Load environment specific configuration"""
        config_path = os.path.join(os.path.dirname(__file__), "../configs/env_config.yaml")
        config = ConfigLoader.load_config(config_path)
        return config['environments'][env]
    
    @staticmethod
    def get_kafka_config() -> Dict[str, Any]:
        """Load Kafka configuration"""
        config_path = os.path.join(os.path.dirname(__file__), "../configs/kafka_config.yaml")
        return ConfigLoader.load_config(config_path)['kafka_config']
    
    @staticmethod
    def get_job_config(job_name: str) -> Dict[str, Any]:
        """Load job specific configuration"""
        config_path = os.path.join(os.path.dirname(__file__), f"../configs/job_config/{job_name}.yaml")
        return ConfigLoader.load_config(config_path)['job']


class SparkUtils:
    @staticmethod
    def init_spark_session(app_name: str, env: str = "dev") -> SparkSession:
        """Initialize Spark session with Delta Lake configuration"""
        env_config = ConfigLoader.get_environment_config(env)
        
        return (SparkSession.builder
                .appName(app_name)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.databricks.delta.optimizeWrite.enabled", "true")
                .config("spark.databricks.delta.autoCompact.enabled", "true")
                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate())

    @staticmethod
    def apply_transformations(df, transformations: list) -> DataFrame:
        """Apply a series of SQL transformations to a DataFrame"""
        for transform in transformations:
            df.createOrReplaceTempView(transform['name'] + "_view")
            df = spark.sql(transform['sql'])
        return df


class DeltaUtils:
    @staticmethod
    def write_delta(df, target_config: Dict[str, Any], env: str = "dev"):
        """Write DataFrame to Delta table based on target configuration"""
        env_config = ConfigLoader.get_environment_config(env)
        catalog = env_config['databricks']['catalog']
        schema = env_config['databricks']['schema_prefix'] + target_config.get('schema', 'default')
        table_name = target_config['table']
        full_table_path = f"{catalog}.{schema}.{table_name}"
        
        writer = (df.write
                 .format("delta")
                 .option("mergeSchema", "true"))
        
        if 'partition_columns' in target_config:
            writer = writer.partitionBy(*target_config['partition_columns'])
            
        load_strategy = target_config.get('load_strategy', 'append').lower()
        
        if load_strategy == 'append':
            writer.mode("append").saveAsTable(full_table_path)
        elif load_strategy == 'overwrite':
            writer.mode("overwrite").saveAsTable(full_table_path)
        elif load_strategy == 'upsert':
            # Requires primary key configuration
            primary_key = target_config['primary_key']
            temp_view = f"temp_{table_name}"
            df.createOrReplaceTempView(temp_view)
            
            spark.sql(f"""
                MERGE INTO {full_table_path} target
                USING {temp_view} source
                ON target.{primary_key} = source.{primary_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        elif load_strategy == 'delete_insert':
            # Delete existing data before insert
            spark.sql(f"DELETE FROM {full_table_path}")
            writer.mode("append").saveAsTable(full_table_path)
        
        # Run optimization if configured
        if target_config.get('optimize_settings', {}).get('auto_optimize', False):
            DeltaOptimizer.optimize_table(full_table_path, target_config['optimize_settings'])


class DeltaOptimizer:
    @staticmethod
    def optimize_table(table_path: str, optimize_config: Dict[str, Any]):
        """Optimize Delta table based on configuration"""
        zorder_cols = optimize_config.get('zorder_columns', [])
        
        if zorder_cols:
            spark.sql(f"OPTIMIZE {table_path} ZORDER BY ({','.join(zorder_cols)})")
        else:
            spark.sql(f"OPTIMIZE {table_path}")
        
        if optimize_config.get('vacuum', {}).get('enabled', False):
            retain_hours = optimize_config['vacuum'].get('retain_hours', 168)
            spark.sql(f"VACUUM {table_path} RETAIN {retain_hours} HOURS")