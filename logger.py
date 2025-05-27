import logging
from logging.handlers import RotatingFileHandler
from typing import Optional
import os
from datetime import datetime
from pyspark.sql import SparkSession

class ETLLogger:
    _instance = None
    
    def __new__(cls, spark: Optional[SparkSession] = None, log_dir: str = "logs"):
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
            cls._instance._initialize_logger(spark, log_dir)
        return cls._instance
    
    def _initialize_logger(self, spark: Optional[SparkSession], log_dir: str):
        """Initialize logger with both file and Spark logging"""
        self.logger = logging.getLogger("ETLFramework")
        self.logger.setLevel(logging.INFO)
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # File handler
        log_file = os.path.join(log_dir, f"etl_{datetime.now().strftime('%Y%m%d')}.log")
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        
        # Spark logger if available
        self.spark_logger = None
        if spark:
            log4j_logger = spark._jvm.org.apache.log4j
            self.spark_logger = log4j_logger.LogManager.getLogger("ETLFramework")
        
        self.logger.addHandler(file_handler)
    
    def info(self, message: str):
        self.logger.info(message)
        if self.spark_logger:
            self.spark_logger.info(message)
    
    def warning(self, message: str):
        self.logger.warning(message)
        if self.spark_logger:
            self.spark_logger.warn(message)
    
    def error(self, message: str, exc_info=None):
        self.logger.error(message, exc_info=exc_info)
        if self.spark_logger:
            self.spark_logger.error(message)
            if exc_info:
                self.spark_logger.error(exc_info)
    
    def exception(self, message: str):
        self.logger.exception(message)
        if self.spark_logger:
            self.spark_logger.error(message)