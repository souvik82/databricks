# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 4: Silver to Gold Load (Streaming)

# COMMAND ----------

# DBTITLE 1,Initialize
from libs.utils import ConfigLoader, SparkUtils, DeltaUtils
from libs.delta_optimizer import DeltaOptimizer
import os

# Get configurations
job_config = ConfigLoader.get_job_config("gold_load")
env_config = ConfigLoader.get_environment_config()

# Initialize Spark
spark = SparkUtils.init_spark_session("Silver_to_Gold_Stream")

# COMMAND ----------

# DBTITLE 1,Read Silver Table as Stream
catalog = env_config['databricks']['catalog']
schema = env_config['databricks']['schema_prefix'] + job_config['source'].get('schema', 'default')
silver_table = f"{catalog}.{schema}.{job_config['source']['table']}"

silver_df = (spark.readStream
             .format("delta")
             .table(silver_table)
             .option("readChangeFeed", "true")
             .option("startingVersion", job_config['source'].get('startingVersion', 'latest')))

# COMMAND ----------

# DBTITLE 1,Apply Transformations
transformed_df = SparkUtils.apply_transformations(silver_df, job_config["transformations"])

# COMMAND ----------

# DBTITLE 1,Write to Gold
gold_table = f"{catalog}.{schema}.{job_config['target']['table']}"

(transformed_df.writeStream
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation", f"{env_config['storage']['checkpoint_location']}/{job_config['target']['table']}_gold")
 .trigger(availableNow=True)
 .toTable(gold_table)
 .awaitTermination())

# Run optimization after stream completes
DeltaOptimizer.optimize_table(gold_table, job_config["target"].get("optimize_settings", {}))