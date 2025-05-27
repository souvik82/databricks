from pyspark.sql.types import StructType, StructField, DataType
from typing import Dict, Any, List
import json
from libs.logger import ETLLogger

class SchemaManager:
    def __init__(self, logger: ETLLogger = None):
        self.logger = logger or ETLLogger()
    
    def create_struct_from_config(self, schema_config: Dict[str, Any]) -> StructType:
        """Create Spark StructType from configuration"""
        self.logger.info("Creating schema from configuration")
        
        fields = []
        for field_name, field_def in schema_config.items():
            field_type = self._parse_type(field_def['type'])
            nullable = field_def.get('nullable', True)
            metadata = field_def.get('metadata', {})
            
            fields.append(StructField(
                field_name,
                field_type,
                nullable,
                metadata
            ))
        
        return StructType(fields)
    
    def _parse_type(self, type_str: str) -> DataType:
        """Convert type string to Spark DataType"""
        from pyspark.sql.types import (
            StringType, IntegerType, LongType, 
            DoubleType, BooleanType, TimestampType,
            DateType, ArrayType, MapType
        )
        
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'long': LongType(),
            'double': DoubleType(),
            'boolean': BooleanType(),
            'timestamp': TimestampType(),
            'date': DateType()
        }
        
        # Handle array types
        if type_str.startswith('array<') and type_str.endswith('>'):
            element_type = self._parse_type(type_str[6:-1])
            return ArrayType(element_type)
        
        # Handle map types
        if type_str.startswith('map<') and type_str.endswith('>'):
            key_value_types = type_str[4:-1].split(',')
            if len(key_value_types) != 2:
                raise ValueError(f"Invalid map type definition: {type_str}")
            key_type = self._parse_type(key_value_types[0].strip())
            value_type = self._parse_type(key_value_types[1].strip())
            return MapType(key_type, value_type)
        
        # Handle simple types
        if type_str in type_mapping:
            return type_mapping[type_str]
        
        raise ValueError(f"Unsupported type: {type_str}")
    
    def save_schema_to_file(self, schema: StructType, path: str):
        """Save schema definition to file"""
        schema_json = json.dumps(schema.jsonValue())
        with open(path, 'w') as f:
            f.write(schema_json)
        self.logger.info(f"Schema saved to {path}")
    
    def load_schema_from_file(self, path: str) -> StructType:
        """Load schema definition from file"""
        with open(path, 'r') as f:
            schema_json = f.read()
        return StructType.fromJson(json.loads(schema_json))