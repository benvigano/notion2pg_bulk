"""
Schema mapping utilities for converting Notion properties to PostgreSQL columns.
"""

from typing import Dict, List, Any, Optional
from sqlalchemy import Column, String, Text, Numeric, Boolean, TIMESTAMP, ARRAY



class NotionPropertyMapper:
    """Maps Notion property types to PostgreSQL column definitions."""
    
    @staticmethod
    def get_postgres_column(property_name: str, property_config: Dict[str, Any]) -> Optional[Column]:
        """
        Convert a Notion property to a PostgreSQL column.
        
        Args:
            property_name: Name of the property
            property_config: Notion property configuration
            
        Returns:
            SQLAlchemy Column object or None if property should be skipped
        """
        property_type = property_config.get("type")
        
        # Skip computed properties
        if property_type in ["formula", "rollup"]:
            return None
            
        column_kwargs = {"comment": property_config.get("description", "")}
        
        mapping = {
            "title": Text,
            "rich_text": Text,
            "number": Numeric,
            "select": String(255),
            "multi_select": ARRAY(String(255)),
            "date": TIMESTAMP,
            "checkbox": Boolean,
            "url": Text,
            "email": String(200),
            "phone_number": String(200),
            "relation": ARRAY(String(36)),  # UUIDs as strings
            "people": ARRAY(String(36)),    # User IDs
            "files": ARRAY(Text),           # File URLs
            "created_time": TIMESTAMP,
            "created_by": String(36),
            "last_edited_time": TIMESTAMP,
            "last_edited_by": String(36)
        }
        
        column_type = mapping.get(property_type, Text)
        return Column(property_name, column_type, **column_kwargs)
    
    @staticmethod
    def needs_lookup_table(property_config: Dict[str, Any]) -> bool:
        """Check if a property type needs a separate option table for select choices."""
        return property_config.get("type") in ["select", "multi_select"]
    
    @staticmethod
    def extract_property_value(property_data: Dict[str, Any], property_type: str) -> Any:
        """
        Extract and convert property value from Notion API response.
        
        Args:
            property_data: Property data from Notion API
            property_type: Type of the property
            
        Returns:
            Converted value suitable for PostgreSQL
        """
        if not property_data:
            return None
            
        extractors = {
            "title": lambda x: NotionPropertyMapper._extract_rich_text(x.get("title", [])),
            "rich_text": lambda x: NotionPropertyMapper._extract_rich_text(x.get("rich_text", [])),
            "number": lambda x: x.get("number"),
            "select": lambda x: x.get("select", {}).get("name") if x.get("select") else None,
            "multi_select": lambda x: [opt.get("name") for opt in x.get("multi_select", [])],
            "date": lambda x: x.get("date", {}).get("start") if x.get("date") else None,
            "checkbox": lambda x: x.get("checkbox", False),
            "url": lambda x: x.get("url"),
            "email": lambda x: x.get("email"),
            "phone_number": lambda x: x.get("phone_number"),
            "relation": lambda x: [rel.get("id") for rel in x.get("relation", [])],
            "people": lambda x: [person.get("id") for person in x.get("people", [])],
            "files": lambda x: [file.get("external", {}).get("url") or file.get("file", {}).get("url") 
                               for file in x.get("files", []) if file],
            "created_time": lambda x: x.get("created_time"),
            "created_by": lambda x: x.get("created_by", {}).get("id"),
            "last_edited_time": lambda x: x.get("last_edited_time"),
            "last_edited_by": lambda x: x.get("last_edited_by", {}).get("id")
        }
        
        extractor = extractors.get(property_type, lambda x: str(x))
        return extractor(property_data)
    
    @staticmethod
    def _extract_rich_text(rich_text_array: List[Dict]) -> str:
        """Extract plain text from Notion rich text array."""
        if not rich_text_array:
            return ""
        return "".join(item.get("plain_text", "") for item in rich_text_array)
    
    @staticmethod
    def get_lookup_table_name(table_name: str, field_name: str) -> str:
        """Generate option table name for select and multi-select fields."""
        # Import here to avoid circular imports
        import re
        
        # Clean field name to be PostgreSQL compatible
        cleaned_field = re.sub(r'[^\w]', '_', field_name)
        cleaned_field = re.sub(r'_+', '_', cleaned_field)
        cleaned_field = cleaned_field.strip('_').lower()
        
        if not cleaned_field or cleaned_field[0].isdigit():
            cleaned_field = f"field_{cleaned_field}"
        
        # Use double underscore for clear separation
        return f"{table_name}__{cleaned_field}"
