# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1: Extract from Kafka to Raw Storage

# COMMAND ----------

# DBTITLE 1,Initialize
from libs.utils import ConfigLoader, SparkUtils
from libs.delta_optimizer import DeltaOptimizer
import os

# Get job configuration
job_config = ConfigLoader.get_job_config("bronze_load")
kafka_config = ConfigLoader.get_kafka_config()
env_config = ConfigLoader.get_environment_config()

# Initialize Spark
spark = SparkUtils.init_spark_session("Kafka_to_Raw_Extract")

# COMMAND ----------

# DBTITLE 1,Read from Kafka
# Set up Kafka options
kafka_options = {
    "kafka.bootstrap.servers": kafka_config["bootstrap_servers"],
    "subscribe": job_config["source"]["topic"],
    "startingOffsets": job_config["source"]["options"].get("startingOffsets", "earliest"),
    "kafka.security.protocol": kafka_config["security_protocol"],
    "kafka.sasl.mechanism": kafka_config["sasl_mechanism"],
    "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.scram.ScramLoginModule required username='{kafka_config['rbac_username']}' password='{kafka_config['rbac_password']}';",
    "group.id": kafka_config["consumer_group_id"]
}

# Read from Kafka
df = (spark.readStream
      .format("kafka")
      .options(**kafka_options)
      .load())

# COMMAND ----------

# DBTITLE 1,Write to Raw Storage
raw_path = f"abfss://{env_config['storage']['raw_container']}@<storage_account>.dfs.core.windows.net/{job_config['source']['topic']}"

(df.writeStream
 .format("json")
 .option("path", raw_path)
 .option("checkpointLocation", f"{env_config['storage']['checkpoint_location']}/{job_config['source']['topic']}_raw")
 .outputMode("append")
 .start()
 .awaitTermination())