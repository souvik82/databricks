# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 2: Raw to Bronze Load

# COMMAND ----------

# DBTITLE 1,Initialize
from libs.config_loader import ConfigLoader
from libs.readers.file_reader import FileReader
from libs.writers.delta_writer import DeltaWriter
from libs.transformers import apply_transformations
from libs.logger import ETLLogger

# Initialize logger
logger = ETLLogger(spark)

try:
    # Get configurations
    job_config = ConfigLoader.get_job_config("bronze_load")
    env_config = ConfigLoader.get_environment_config()
    
    # Initialize reader and writer
    reader = FileReader({
        "path": f"abfss://{env_config['storage']['raw_container']}@<storage_account>.dfs.core.windows.net/{job_config['source']['topic']}",
        "format": "json",
        "schema_location": f"{env_config['storage']['checkpoint_location']}/{job_config['target']['table']}_schema"
    }, logger)
    
    writer = DeltaWriter({
        "catalog": env_config['databricks']['catalog'],
        "schema": env_config['databricks']['schema_prefix'] + job_config['target'].get('schema', 'default'),
        "table": job_config['target']['table'],
        "partition_columns": job_config['target'].get('partition_columns', []),
        "mode": job_config['target'].get('mode', 'append'),
        "checkpoint_location": f"{env_config['storage']['checkpoint_location']}/{job_config['target']['table']}_bronze"
    }, logger)

    # COMMAND ----------

    # DBTITLE 1,Read Raw Data
    logger.info("Starting raw data read")
    raw_df = reader.read()
    logger.info(f"Read {raw_df.count()} rows from raw storage")

    # COMMAND ----------

    # DBTITLE 1,Apply Transformations
    logger.info("Applying transformations")
    transformed_df = apply_transformations(
        raw_df, 
        job_config["transformations"],
        logger
    )
    logger.info("Transformations completed successfully")

    # COMMAND ----------

    # DBTITLE 1,Write to Bronze
    logger.info("Starting bronze table write")
    writer.write(transformed_df)
    logger.info("Bronze table write completed successfully")

except Exception as e:
    logger.error("Error in raw to bronze processing", exc_info=e)
    raise

# COMMAND ----------

# DBTITLE 1,Archive Raw Data (Optional)
if job_config["archive"].get("enabled", False):
    try:
        from libs.writers.file_writer import FileWriter
        
        logger.info("Starting raw data archiving")
        archive_writer = FileWriter({
            "path": f"abfss://{env_config['storage']['archive_container']}@<storage_account>.dfs.core.windows.net/{job_config['archive']['location']}",
            "format": job_config["archive"]["format"],
            "mode": job_config["archive"]["mode"]
        }, logger)
        
        archive_writer.write(raw_df)
        logger.info("Raw data archived successfully")
    except Exception as e:
        logger.error("Error during archiving", exc_info=e)
        raise