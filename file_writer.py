from pyspark.sql import DataFrame
from libs.writers.base_writer import BaseWriter
from libs.error_handler import ETLErrorHandler
from typing import Optional

class FileWriter(BaseWriter):
    @ETLErrorHandler.handle_spark_errors()
    def write(self, df: DataFrame) -> None:
        """Write batch data to files"""
        self.logger.info(f"Starting batch write to {self.config['path']}")
        
        writer = df.write.format(self.config['format'])
        
        # Apply write mode
        mode = self.config.get('mode', 'append')
        writer = writer.mode(mode)
        
        # Apply partitioning if configured
        if 'partition_columns' in self.config:
            writer = writer.partitionBy(*self.config['partition_columns'])
            self.logger.info(f"Using partition columns: {self.config['partition_columns']}")
        
        # Apply options
        for opt, val in self.config.get('options', {}).items():
            writer = writer.option(opt, val)
        
        # Save the data
        writer.save(self.config['path'])
        self.logger.info(f"Successfully wrote batch data to {self.config['path']}")

    @ETLErrorHandler.handle_spark_errors()
    def write_stream(self, df: DataFrame) -> Optional[Any]:
        """Write streaming data to files"""
        self.logger.info(f"Starting stream write to {self.config['path']}")
        
        checkpoint_location = self.config.get('checkpoint_location')
        if not checkpoint_location:
            raise ValueError("checkpoint_location is required for streaming writes")
        
        writer = df.writeStream \
            .format(self.config['format']) \
            .option("checkpointLocation", checkpoint_location)
        
        # Apply output mode
        mode = self.config.get('mode', 'append')
        writer = writer.outputMode(mode)
        
        # Apply partitioning if configured
        if 'partition_columns' in self.config:
            writer = writer.partitionBy(*self.config['partition_columns'])
            self.logger.info(f"Using partition columns: {self.config['partition_columns']}")
        
        # Apply options
        for opt, val in self.config.get('options', {}).items():
            writer = writer.option(opt, val)
        
        # Start the stream
        query = writer.start(self.config['path'])
        
        self.logger.info(f"Started streaming write to {self.config['path']} with query ID: {query.id}")
        return query