from pyspark.sql import DataFrame
from libs.readers.base_reader import BaseReader
from libs.error_handler import ETLErrorHandler
from typing import Dict, Any

class FileReader(BaseReader):
    @ETLErrorHandler.handle_spark_errors()
    def read(self) -> DataFrame:
        """Read batch data from files"""
        self.logger.info(f"Starting batch read from {self.config['path']}")
        
        reader = self.spark.read.format(self.config['format'])
        
        # Apply schema if provided
        if 'schema' in self.config:
            reader = reader.schema(self.config['schema'])
        
        # Apply options
        for opt, val in self.config.get('options', {}).items():
            reader = reader.option(opt, val)
        
        # CloudFiles specific configuration
        if self.config.get('cloud_files', {}).get('enabled', False):
            reader = reader.option("cloudFiles.format", self.config['format'])
            if 'schema_location' in self.config:
                reader = reader.option("cloudFiles.schemaLocation", self.config['schema_location'])
            
            for opt, val in self.config['cloud_files'].items():
                if opt != 'enabled':
                    reader = reader.option(f"cloudFiles.{opt}", val)
            
            df = reader.load(self.config['path'])
        else:
            df = reader.load(self.config['path'])
        
        self.logger.info(f"Successfully read {df.count()} rows from {self.config['path']}")
        return df

    @ETLErrorHandler.handle_spark_errors()
    def read_stream(self) -> DataFrame:
        """Read streaming data from files using CloudFiles"""
        self.logger.info(f"Starting stream read from {self.config['path']}")
        
        if not self.config.get('cloud_files', {}).get('enabled', False):
            raise ValueError("CloudFiles must be enabled for streaming reads")
        
        reader = self.spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", self.config['format'])
        
        if 'schema_location' in self.config:
            reader = reader.option("cloudFiles.schemaLocation", self.config['schema_location'])
        
        # Apply options
        for opt, val in self.config.get('options', {}).items():
            reader = reader.option(opt, val)
        
        for opt, val in self.config['cloud_files'].items():
            if opt != 'enabled':
                reader = reader.option(f"cloudFiles.{opt}", val)
        
        df = reader.load(self.config['path'])
        
        self.logger.info(f"Successfully started streaming from {self.config['path']}")
        return df