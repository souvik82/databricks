# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 3: Bronze to Silver Load

# COMMAND ----------

# DBTITLE 1,Initialize
from libs.config_loader import ConfigLoader
from libs.path_builder import PathBuilder
from libs.readers.delta_reader import DeltaReader
from libs.writers.delta_writer import DeltaWriter
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
checkpoint_path = path_builder.get_checkpoint_path(table_name)

try:
    # COMMAND ----------
    # DBTITLE 1,Configure Source and Target
    reader = DeltaReader({
        "catalog": job_config['source']['catalog'],
        "schema": job_config['source']['schema'],
        "table": job_config['source']['table'],
        "startingVersion": job_config['source'].get('startingVersion', 'latest'),
        "options": job_config['source'].get('options', {})
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
    # DBTITLE 1,Execute Streaming Pipeline
    bronze_df = reader.read_stream()
    transformed_df = apply_transformations(
        bronze_df,
        job_config["transformations"],
        job_config["target"],
        env_config,
        logger
    )

    if transformed_df is not None:
        writer.write_stream(transformed_df).awaitTermination()

except Exception as e:
    logger.error("Bronze to Silver processing failed", exc_info=e)
    raise
finally:
    widgets_mgr.remove_widgets()