# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 2: ADLS to Bronze Load

# COMMAND ----------

# DBTITLE 1,Initialize
from libs.config_loader import ConfigLoader
from libs.readers.file_reader import FileReader
from libs.writers.delta_writer import DeltaWriter
from libs.writers.file_writer import FileWriter
from libs.schema_manager import SchemaManager
from libs.transformers import apply_transformations
from libs.logger import ETLLogger
from libs.widgets_manager import WidgetsManager
from datetime import datetime

# Initialize services
logger = ETLLogger(spark)
widgets_mgr = WidgetsManager(logger)
widgets_mgr.create_widgets()

# Get configurations
env = widgets_mgr.get_environment()
job_config_name = widgets_mgr.get_job_config_name()
force_reprocess = widgets_mgr.get_force_reprocess()

job_config = ConfigLoader.get_job_config(job_config_name)
env_config = ConfigLoader.get_environment_config(env)

# Substitute variables in config
processing_date = datetime.now().strftime("%Y-%m-%d")
job_config = _substitute_config_vars(job_config, {
    "environment": env,
    "date": processing_date,
    "force_reprocess": str(force_reprocess).lower()
})

logger.info(f"Starting {job_config['name']} for {env} environment")

try:
    # COMMAND ----------
    # DBTITLE 1,Configure Source and Target
    # Initialize schema manager
    schema_manager = SchemaManager(logger)
    
    # Initialize reader with CloudFiles
    reader = FileReader({
        "path": f"abfss://{env_config['storage']['raw_container']}@<storage_account>.dfs.core.windows.net/{job_config['source']['path']}",
        "format": job_config['source']['format'],
        "schema_location": f"{env_config['storage']['checkpoint_location']}/{job_config['target']['table']}_schema",
        **job_config['source']['options']
    }, logger)
    
    # Initialize Delta writer
    writer = DeltaWriter({
        "catalog": env_config['databricks']['catalog'],
        "schema": env_config['databricks']['schema_prefix'] + job_config['target'].get('schema', 'default'),
        "table": job_config['target']['table'],
        "partition_columns": job_config['target'].get('partition_columns', []),
        "mode": job_config['target'].get('load_strategy', 'append'),
        "checkpoint_location": job_config['checkpoint']['location'],
        "force_reset": job_config['checkpoint'].get('force_reset', 'false')
    }, logger)

    # COMMAND ----------
    # DBTITLE 1,Read and Transform Data
    logger.info("Starting ADLS to Bronze processing")
    
    # Read from ADLS using CloudFiles
    raw_df = reader.read()
    
    # Apply transformations with merge/upsert support
    transformed_df = apply_transformations(
        raw_df,
        job_config["transformations"],
        job_config["target"],
        env_config,
        logger
    )
    
    # Only write if not using merge/delete-insert strategy
    if transformed_df is not None:
        writer.write(transformed_df)

    # COMMAND ----------
    # DBTITLE 1,Archive Processed Files
    if job_config["archive"].get("enabled", False):
        logger.info("Starting file archiving")
        _archive_processed_files(spark, job_config, env_config, logger)

except Exception as e:
    logger.error("ADLS to Bronze processing failed", exc_info=e)
    if job_config["controls"].get("notify_on_failure", False):
        _send_failure_notification(job_config['name'], str(e))
    raise
finally:
    widgets_mgr.remove_widgets()

# COMMAND ----------
# Helper functions
def _substitute_config_vars(config: dict, variables: dict) -> dict:
    """Replace {{var}} in config with actual values"""
    import json
    config_str = json.dumps(config)
    for var, value in variables.items():
        config_str = config_str.replace(f'"{{{{{var}}}}}"', f'"{value}"')
    return json.loads(config_str)

def _archive_processed_files(spark, job_config, env_config, logger):
    """Archive processed files to archive location"""
    from libs.writers.file_writer import FileWriter
    
    # Get list of processed files
    processed_files = spark.sql("SELECT DISTINCT _source_file FROM bronze_final_view").collect()
    file_list = [f._source_file for f in processed_files]
    
    if file_list:
        # Initialize archive writer
        archive_writer = FileWriter({
            "path": f"abfss://{env_config['storage']['archive_container']}@<storage_account>.dfs.core.windows.net/{job_config['archive']['path']}",
            "format": job_config['archive']['format'],
            "mode": job_config['archive']['mode']
        }, logger)
        
        # Read and archive each file
        for file_path in file_list:
            file_df = spark.read.format(job_config['source']['format']).load(file_path)
            archive_writer.write(file_df)
            
            # Optional: Delete source file after archiving
            # dbutils.fs.rm(file_path)
        
        logger.info(f"Archived {len(file_list)} files")
    else:
        logger.warning("No files to archive")

def _send_failure_notification(job_name: str, error_msg: str):
    """Send failure notification"""
    # Implement your notification logic (email, Teams, Slack, etc.)
    print(f"ALERT: Job {job_name} failed with error: {error_msg}")