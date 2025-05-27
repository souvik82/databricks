# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1: Kafka to ADLS Extraction

# COMMAND ----------

# DBTITLE 1,Initialize
from libs.config_loader import ConfigLoader
from libs.readers.kafka_reader import KafkaReader
from libs.writers.file_writer import FileWriter
from libs.schema_manager import SchemaManager
from libs.logger import ETLLogger

# Initialize services
logger = ETLLogger(spark)
config = ConfigLoader.get_job_config("kafka_to_adls")
env_config = ConfigLoader.get_environment_config()

try:
    # COMMAND ----------
    # DBTITLE 1,Configure Source and Target
    # Create schema from config
    schema_manager = SchemaManager(logger)
    schema = schema_manager.create_struct_from_config(config['source']['schema'])
    
    # Initialize reader and writer
    kafka_config = ConfigLoader.get_kafka_config()
    reader = KafkaReader({
        **kafka_config,
        "topic": config['source']['topic'],
        **config['source']['options']
    }, logger)
    
    writer = FileWriter({
        "path": f"abfss://{env_config['storage']['raw_container']}@<storage_account>.dfs.core.windows.net/{config['target']['path']}",
        "format": config['target']['format'],
        **config['target']['options']
    }, logger)

    # COMMAND ----------
    # DBTITLE 1,Execute Streaming Job
    logger.info("Starting Kafka to ADLS streaming job")
    
    # Read from Kafka
    kafka_df = reader.read_stream()
    
    # Write to ADLS
    query = writer.write_stream(kafka_df)
    
    # Wait for termination (in production you might not want this)
    query.awaitTermination()
    
    logger.info("Kafka to ADLS job completed successfully")

except Exception as e:
    logger.error("Failed to execute Kafka to ADLS job", exc_info=e)
    raise