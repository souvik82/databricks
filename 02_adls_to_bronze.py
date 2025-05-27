# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 2: ADLS to Bronze Load

# COMMAND ----------

# DBTITLE 1,Initialize
from libs.config_loader import ConfigLoader
from libs.path_builder import PathBuilder
from libs.readers.file_reader import FileReader
from libs.writers.delta_writer import DeltaWriter
from libs.writers.file_writer import FileWriter
from libs.transformers import apply_transformations
from libs.logger import ETLLogger
from libs.widgets_manager import WidgetsManager

# Initialize services
logger = ETLLogger(spark)
widgets_mgr = WidgetsManager(logger)
widgets_mgr.create_widgets()

# Get configurations
env = widgets_mgr.get_environment()
job_config_name = widgets_mgr.get_job_config_name()
job_config = ConfigLoader().get_job_config(job_config_name)
env_config = ConfigLoader().get_environment_config(env)
path_builder = PathBuilder(env_config)

# Build paths
table_name = job_config['target']['table']
source_path = path_builder.get_raw_path(job_config['source'].get('path', ''))
checkpoint_path = path_builder.get_checkpoint_path(table_name)
schema_path = path_builder.get_schema_path(table_name)
archive_path = path_builder.get_archive_path(table_name)

try:
    # COMMAND ----------
    # DBTITLE 1,Configure Source and Target
    reader = FileReader({
        "path": source_path,
        "format": job_config['source']['format'],
        "schema_location": schema_path,
        "cloud_files": {
            "use_notifications": True,
            "max_files_per_trigger": job_config.get('max_files_per_trigger', 1000)
        }
    }, logger)

    writer = DeltaWriter({
        "catalog": env_config['databricks']['catalog'],
        "schema": env_config['databricks']['schema_prefix'] + job_config['target'].get('schema', 'default'),
        "table": table_name,
        "partition_columns": job_config['target'].get('partition_columns', []),
        "mode": job_config['target'].get('load_strategy', 'append'),
        "checkpoint_location": checkpoint_path
    }, logger)

    # COMMAND ----------
    # DBTITLE 1,Execute Pipeline
    raw_df = reader.read()
    transformed_df = apply_transformations(
        raw_df,
        job_config["transformations"],
        job_config["target"],
        env_config,
        logger
    )

    if transformed_df is not None:
        writer.write(transformed_df)

    # Archive processed files
    if job_config["archive"].get("enabled", False):
        archive_writer = FileWriter({
            "path": archive_path,
            "format": job_config["archive"]["format"],
            "mode": job_config["archive"]["mode"]
        }, logger)
        
        processed_files = spark.sql("SELECT DISTINCT _source_file FROM bronze_final_view").collect()
        for file in processed_files:
            file_df = spark.read.format(job_config['source']['format']).load(file._source_file)
            archive_writer.write(file_df)

except Exception as e:
    logger.error("ADLS to Bronze processing failed", exc_info=e)
    raise
finally:
    widgets_mgr.remove_widgets()