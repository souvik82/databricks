from pyspark.sql import DataFrame
from libs.readers.base_reader import BaseReader
from libs.error_handler import ETLErrorHandler

class KafkaReader(BaseReader):
    @ETLErrorHandler.handle_spark_errors()
    def read(self) -> DataFrame:
        """Read batch data from Kafka"""
        self.logger.info(f"Starting batch read from Kafka topic: {self.config['topic']}")
        
        df = self._create_reader() \
            .option("endingOffsets", self.config.get("endingOffsets", "latest")) \
            .load()
            
        self.logger.info(f"Successfully read batch data from Kafka topic: {self.config['topic']}")
        return df

    @ETLErrorHandler.handle_spark_errors()
    def read_stream(self) -> DataFrame:
        """Read streaming data from Kafka"""
        self.logger.info(f"Starting stream read from Kafka topic: {self.config['topic']}")
        
        df = self._create_reader() \
            .option("startingOffsets", self.config.get("startingOffsets", "latest")) \
            .load()
            
        self.logger.info(f"Successfully started streaming from Kafka topic: {self.config['topic']}")
        return df

    def _create_reader(self):
        """Create configured Kafka reader"""
        return self.spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config["bootstrap_servers"]) \
            .option("subscribe", self.config["topic"]) \
            .option("kafka.security.protocol", self.config["security_protocol"]) \
            .option("kafka.sasl.mechanism", self.config["sasl_mechanism"]) \
            .option("kafka.sasl.jaas.config", 
                   f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                   f"username='{self.config['rbac_username']}' "
                   f"password='{self.config['rbac_password']}';") \
            .option("group.id", self.config["consumer_group_id"])