from typing import Dict, Any
from dbutils import widgets
import json
import os
from libs.logger import ETLLogger

class WidgetsManager:
    def __init__(self, logger: ETLLogger = None):
        self.logger = logger or ETLLogger()
        self.widgets_config = self._load_widgets_config()
    
    def _load_widgets_config(self) -> Dict[str, Any]:
        """Load widget configuration from JSON file"""
        config_path = os.path.join(os.path.dirname(__file__), "../../widgets/job_control.json")
        with open(config_path) as f:
            return json.load(f)
    
    def create_widgets(self):
        """Create all defined widgets"""
        for name, config in self.widgets_config.items():
            if config["type"] == "dropdown":
                widgets.dropdown(name, config["default"], config["choices"], config["description"])
            elif config["type"] == "text":
                widgets.text(name, config["default"], config["description"])
            elif config["type"] == "checkbox":
                widgets.combobox(name, config["default"], [True, False], config["description"])
        
        self.logger.info("Created all job control widgets")
    
    def get_environment(self) -> str:
        """Get selected environment"""
        return widgets.get("environment")
    
    def get_job_config_name(self) -> str:
        """Get selected job config name"""
        return widgets.get("job_config")
    
    def get_force_reprocess(self) -> bool:
        """Get force reprocess flag"""
        return widgets.get("force_reprocess") == "true"
    
    def remove_widgets(self):
        """Remove all widgets"""
        widgets.removeAll()
        self.logger.info("Removed all widgets")