from functools import wraps
from typing import Callable, Any
from libs.logger import ETLLogger
import traceback

class ETLErrorHandler:
    @staticmethod
    def handle_errors(logger: ETLLogger = None):
        """Decorator to handle errors and log them appropriately"""
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if logger:
                        logger.error(
                            f"Error in {func.__name__}: {str(e)}",
                            exc_info=traceback.format_exc()
                        )
                    raise  # Re-raise the exception after logging
            return wrapper
        return decorator

    @staticmethod
    def handle_spark_errors(logger: ETLLogger = None):
        """Special handler for Spark operations with checkpoint recovery"""
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if logger:
                        logger.error(
                            f"Spark operation failed in {func.__name__}: {str(e)}",
                            exc_info=traceback.format_exc()
                        )
                    
                    # Add any Spark-specific recovery logic here
                    # For example, you might want to clean up temporary views
                    
                    raise
            return wrapper
        return decorator