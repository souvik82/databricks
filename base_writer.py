from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict, Any
from libs.logger import ETLLogger
from libs.error_handler import ETLErrorHandler

class BaseWriter(ABC):
    def __init__(self, config: Dict[str, Any], logger: ETLLogger = None):
        self.config = config
        self.logger = logger or ETLLogger()
        self.error_handler = ETLErrorHandler()
    
    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Write batch data to target"""
        pass

    @abstractmethod
    def write_stream(self, df: DataFrame) -> None:
        """Write streaming data to target"""
        pass