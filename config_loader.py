import yaml
import os
from typing import Dict, Any
from string import Template
from libs.logger import ETLLogger
from databricks.sdk import WorkspaceClient

class ConfigLoader:
    def __init__(self, logger: ETLLogger = None):
        self.logger = logger or ETLLogger()
        self.config_dir = os.path.join(os.path.dirname(__file__), "../configs")
        self.uc_client = WorkspaceClient()  # Unity Catalog client
    
    def _get_uc_table_path(self, catalog: str, schema: str, table: str) -> str:
        """Get fully qualified Unity Catalog table path"""
        return f"{catalog}.{schema}.{table}"
    
    def _validate_uc_permissions(self, catalog: str, schema: str):
        """Validate Unity Catalog permissions"""
        try:
            # Check if we have USAGE permission on catalog
            self.uc_client.catalogs.get(catalog)
            # Check if we have USAGE permission on schema
            self.uc_client.schemas.get(full_name=f"{catalog}.{schema}")
            return True
        except Exception as e:
            self.logger.error(f"Unity Catalog permission validation failed: {str(e)}")
            return False
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load YAML configuration file with UC validation"""
        full_path = os.path.join(self.config_dir, config_path)
        
        try:
            with open(full_path, 'r') as file:
                config = yaml.safe_load(file)
                
                # Validate UC permissions if UC tables are referenced
                if 'target' in config.get('job', {}) and config['job']['target'].get('type') == 'delta':
                    target = config['job']['target']
                    catalog = target.get('catalog', 'hive_metastore')
                    if catalog != 'hive_metastore':  # Only validate for UC catalogs
                        schema = target.get('schema', 'default')
                        if not self._validate_uc_permissions(catalog, schema):
                            raise PermissionError(f"Missing UC permissions on {catalog}.{schema}")
                
                return config
        except Exception as e:
            self.logger.error(f"Failed to load config from {config_path}", exc_info=e)
            raise