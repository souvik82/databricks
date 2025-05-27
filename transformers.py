from pyspark.sql import DataFrame
from libs.logger import ETLLogger
from libs.error_handler import ETLErrorHandler
from typing import Dict, Any

class DeltaTableManager:
    @staticmethod
    @ETLErrorHandler.handle_spark_errors()
    def apply_merge_strategy(
        df: DataFrame,
        target_config: Dict[str, Any],
        env_config: Dict[str, Any],
        logger: ETLLogger
    ) -> None:
        """Apply merge/upsert strategy to target table"""
        catalog = env_config['databricks']['catalog']
        schema = env_config['databricks']['schema_prefix'] + target_config.get('schema', 'default')
        table_name = target_config['table']
        full_table_path = f"{catalog}.{schema}.{table_name}"
        
        # Create temp view
        temp_view = f"merge_{table_name}_temp"
        df.createOrReplaceTempView(temp_view)
        
        # Build merge condition
        merge_condition = " AND ".join(
            [f"target.{pk} = source.{pk}" for pk in target_config['primary_key']]
        )
        
        # Build update set clause
        update_set = ", ".join(
            [f"target.{col} = source.{col}" 
             for col in df.columns 
             if col not in target_config.get('exclude_columns', [])]
        )
        
        # Execute merge
        merge_sql = f"""
            MERGE INTO {full_table_path} target
            USING {temp_view} source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT *
        """
        
        logger.info(f"Executing merge statement:\n{merge_sql}")
        spark.sql(merge_sql)
        logger.info(f"Merge completed for {full_table_path}")

    @staticmethod
    @ETLErrorHandler.handle_spark_errors()
    def apply_delete_insert_strategy(
        df: DataFrame,
        target_config: Dict[str, Any],
        env_config: Dict[str, Any],
        logger: ETLLogger
    ) -> None:
        """Apply delete-insert strategy to target table"""
        catalog = env_config['databricks']['catalog']
        schema = env_config['databricks']['schema_prefix'] + target_config.get('schema', 'default')
        table_name = target_config['table']
        full_table_path = f"{catalog}.{schema}.{table_name}"
        
        # Build delete condition
        delete_condition = " OR ".join(
            [f"source.{pk} = target.{pk}" for pk in target_config['primary_key']]
        )
        
        # Execute delete-insert
        temp_view = f"delete_insert_{table_name}_temp"
        df.createOrReplaceTempView(temp_view)
        
        delete_sql = f"""
            DELETE FROM {full_table_path} target
            WHERE EXISTS (
                SELECT 1 FROM {temp_view} source
                WHERE {delete_condition}
            )
        """
        
        logger.info(f"Executing delete statement:\n{delete_sql}")
        spark.sql(delete_sql)
        
        logger.info(f"Inserting new data into {full_table_path}")
        df.write.format("delta").mode("append").saveAsTable(full_table_path)

@ETLErrorHandler.handle_spark_errors()
def apply_transformations(
    df: DataFrame,
    transformations: list,
    target_config: Dict[str, Any],
    env_config: Dict[str, Any],
    logger: ETLLogger = None
) -> DataFrame:
    """Apply transformations with support for merge/upsert and delete-insert"""
    logger = logger or ETLLogger()
    
    current_df = df
    for i, transform in enumerate(transformations, 1):
        view_name = transform.get('view', f"transform_{i}_view")
        
        logger.info(f"Applying transformation {i}: {transform['name']} -> {view_name}")
        
        # Execute SQL transformation
        current_df.createOrReplaceTempView(view_name)
        current_df = spark.sql(transform['sql'])
        
        logger.info(f"Transformation {transform['name']} completed. Schema: {current_df.schema}")
    
    # Apply final write strategy if specified
    load_strategy = target_config.get('load_strategy', 'append').lower()
    
    if load_strategy == 'upsert':
        if 'primary_key' not in target_config:
            raise ValueError("Primary key must be specified for upsert strategy")
        
        DeltaTableManager.apply_merge_strategy(
            current_df, target_config, env_config, logger
        )
        return None  # Data already written
    
    elif load_strategy == 'delete_insert':
        if 'primary_key' not in target_config:
            raise ValueError("Primary key must be specified for delete-insert strategy")
        
        DeltaTableManager.apply_delete_insert_strategy(
            current_df, target_config, env_config, logger
        )
        return None  # Data already written
    
    return current_df  # For append/overwrite strategies