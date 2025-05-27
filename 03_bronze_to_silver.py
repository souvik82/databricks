# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 3: Bronze to Silver Load (Streaming)

# COMMAND ----------

# DBTITLE 1,Initialize
from libs.utils import ConfigLoader, SparkUtils, DeltaUtils
from libs.delta_optimizer import DeltaOptimizer
import os

# Get configurations
job_config = ConfigLoader.get_job_config("silver_load")
env_config = ConfigLoader.get_environment_config()

# Initialize Spark
spark = SparkUtils.init_spark_session("Bronze_to_Silver_Stream")

# COMMAND ----------

# DBTITLE 1,Read Bronze Table as Stream
catalog = env_config['databricks']['catalog']
schema = env_config['databricks']['schema_prefix'] + job_config['source'].get('schema', 'default')
bronze_table = f"{catalog}.{schema}.{job_config['source']['table']}"

bronze_df = (spark.readStream
             .format("delta")
             .table(bronze_table)
             .option("readChangeFeed", "true")
             .option("startingVersion", job_config['source'].get('startingVersion', 'latest')))

# COMMAND ----------

# DBTITLE 1,Apply Transformations
transformed_df = SparkUtils.apply_transformations(bronze_df, job_config["transformations"])

# COMMAND ----------

# DBTITLE 1,Write to Silver
silver_table = f"{catalog}.{schema}.{job_config['target']['table']}"

(transformed_df.writeStream
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation", f"{env_config['storage']['checkpoint_location']}/{job_config['target']['table']}_silver")
 .trigger(availableNow=True)
 .toTable(silver_table)
 .awaitTermination())

# Run optimization after stream completes
DeltaOptimizer.optimize_table(silver_table, job_config["target"].get("optimize_settings", {}))