from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict, Any
from libs.logger import ETLLogger
from libs.error_handler import ETLErrorHandler

class BaseReader(ABC):
    def __init__(self, config: Dict[str, Any], logger: ETLLogger = None):
        self.config = config
        self.logger = logger or ETLLogger()
        self.error_handler = ETLErrorHandler()
    
    @abstractmethod
    def read(self) -> DataFrame:
        """Read data from source"""
        pass

    @abstractmethod
    def read_stream(self) -> DataFrame:
        """Read data as stream from source"""
        pass