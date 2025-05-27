from typing import Dict, Any
from string import Template
from libs.logger import ETLLogger
import os

class PathBuilder:
    def __init__(self, env_config: Dict[str, Any], logger: ETLLogger = None):
        self.env_config = env_config
        self.logger = logger or ETLLogger()
    
    def get_raw_path(self, relative_path: str = "") -> str:
        """Get full raw path with optional relative path"""
        return self._build_path("raw", relative_path)
    
    def get_archive_path(self, table_name: str) -> str:
        """Get archive path for a specific table"""
        return os.path.join(self._build_path("archive"), table_name)
    
    def get_bronze_path(self, table_name: str) -> str:
        """Get bronze path for a specific table"""
        return os.path.join(self._build_path("bronze"), table_name)
    
    def get_silver_path(self, table_name: str) -> str:
        """Get silver path for a specific table"""
        return os.path.join(self._build_path("silver"), table_name)
    
    def get_gold_path(self, table_name: str) -> str:
        """Get gold path for a specific table"""
        return os.path.join(self._build_path("gold"), table_name)
    
    def get_checkpoint_path(self, table_name: str) -> str:
        """Get checkpoint path for a specific table"""
        return os.path.join(self._build_path("checkpoints"), table_name)
    
    def get_schema_path(self, table_name: str) -> str:
        """Get schema path for a specific table"""
        return os.path.join(self._build_path("schemas"), f"{table_name}_schema")
    
    def _build_path(self, path_type: str, relative_path: str = "") -> str:
        """Build full path from environment config"""
        base_path = Template(self.env_config['paths'][path_type]).safe_substitute(
            storage=self.env_config['storage']
        )
        return os.path.join(base_path, relative_path) if relative_path else base_path