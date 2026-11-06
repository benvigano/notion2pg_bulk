"""
Schema mapping utilities for converting Notion properties to PostgreSQL columns.
"""

from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

from psycopg2.extras import DateTimeTZRange
from sqlalchemy import Column, String, Text, Numeric, Boolean, TIMESTAMP, ARRAY
from sqlalchemy.dialects.postgresql import JSONB, TSTZRANGE


def _parse_notion_datetime(s: Optional[str]) -> Optional[datetime]:
    """Parse a Notion date string into a timezone-aware datetime, defaulting to UTC."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


class NotionPropertyMapper:
    """Maps Notion property types to PostgreSQL column definitions."""

    @staticmethod
    def get_postgres_column(property_name: str, property_config: Dict[str, Any]) -> Optional[Column]:
        """
        Convert a Notion property to its primary PostgreSQL column.

        Returns None for unsupported property types (including formula and rollup,
        whose per-row computed values are not stored in content tables).
        """
        property_type = property_config.get("type")

        column_kwargs = {"comment": property_config.get("description") or ""}

        mapping = {
            "title": Text,
            "rich_text": JSONB,
            "number": Numeric,
            "select": String(255),
            "multi_select": ARRAY(String(255)),
            "status": String(255),
            "date": TSTZRANGE,
            "checkbox": Boolean,
            "url": Text,
            "email": String(200),
            "phone_number": String(200),
            "unique_id": Text,
            "relation": ARRAY(String(36)),
            "people": ARRAY(String(36)),
            "files": JSONB,
            "created_time": TIMESTAMP,
            "created_by": String(36),
            "last_edited_time": TIMESTAMP,
            "last_edited_by": String(36),
        }

        column_type = mapping.get(property_type)
        if column_type is None:
            return None
        return Column(property_name, column_type, **column_kwargs)

    @staticmethod
    def needs_lookup_table(property_config: Dict[str, Any]) -> bool:
        """Check if a property type needs a separate option table for select choices."""
        return property_config.get("type") in ["select", "multi_select", "status"]

    @staticmethod
    def extract_property_value(property_data: Dict[str, Any], property_type: str) -> Any:
        """
        Extract and convert a property value from a Notion API page response.

        For `date` properties use extract_date_range instead.
        """
        if not property_data:
            return None

        extractors = {
            "title": lambda x: NotionPropertyMapper._extract_rich_text(x.get("title", [])),
            "rich_text": lambda x: x.get("rich_text", []),
            "number": lambda x: x.get("number"),
            "select": lambda x: x.get("select", {}).get("name") if x.get("select") else None,
            "multi_select": lambda x: [opt.get("name") for opt in x.get("multi_select", [])],
            "status": lambda x: x.get("status", {}).get("name") if x.get("status") else None,
            "checkbox": lambda x: x.get("checkbox", False),
            "url": lambda x: x.get("url"),
            "email": lambda x: x.get("email"),
            "phone_number": lambda x: x.get("phone_number"),
            "unique_id": lambda x: (
                f"{x['unique_id']['prefix']}-{x['unique_id']['number']}"
                if x.get("unique_id") and x["unique_id"].get("prefix")
                else (str(x["unique_id"]["number"]) if x.get("unique_id") else None)
            ),
            "relation": lambda x: [rel.get("id") for rel in x.get("relation", [])],
            "people": lambda x: [person.get("id") for person in x.get("people", [])],
            "files": lambda x: x.get("files", []),
            "created_time": lambda x: x.get("created_time"),
            "created_by": lambda x: x.get("created_by", {}).get("id"),
            "last_edited_time": lambda x: x.get("last_edited_time"),
            "last_edited_by": lambda x: x.get("last_edited_by", {}).get("id"),
        }

        extractor = extractors.get(property_type)
        if extractor is None:
            return None
        return extractor(property_data)

    @staticmethod
    def extract_date_range(property_data: Dict[str, Any]) -> Optional[DateTimeTZRange]:
        """Extract the TSTZRANGE value from a Notion date property."""
        if not property_data:
            return None
        date_obj = property_data.get("date")
        if not date_obj:
            return None
        start_dt = _parse_notion_datetime(date_obj.get("start"))
        if start_dt is None:
            return None
        end_str = date_obj.get("end")
        end_dt = _parse_notion_datetime(end_str) if end_str else start_dt
        return DateTimeTZRange(start_dt, end_dt, bounds="[]")

    @staticmethod
    def _extract_rich_text(rich_text_array: List[Dict]) -> str:
        """
        Extract markdown-formatted text from a Notion rich text array.

        Text-type items are formatted with annotations. Mention and equation items
        fall back to Notion's pre-rendered plain_text so they are never silently dropped.
        """
        if not rich_text_array:
            return ""

        markdown_parts = []
        for item in rich_text_array:
            item_type = item.get("type", "text")

            if item_type != "text":
                # Mention (@page, @user, @date) and inline equation — use plain_text as-is.
                plain = item.get("plain_text", "")
                if plain:
                    markdown_parts.append(plain)
                continue

            text_content = item.get("text", {}).get("content", "")
            if not text_content:
                # Unexpected empty text item — fall back to plain_text.
                plain = item.get("plain_text", "")
                if plain:
                    markdown_parts.append(plain)
                continue

            annotations = item.get("annotations", {})
            link_url = item.get("href")
            formatted_text = text_content

            if annotations.get("code"):
                formatted_text = f"`{formatted_text}`"
            if annotations.get("strikethrough"):
                formatted_text = f"~~{formatted_text}~~"
            if annotations.get("bold"):
                formatted_text = f"**{formatted_text}**"
            if annotations.get("italic"):
                formatted_text = f"*{formatted_text}*"
            if annotations.get("underline"):
                formatted_text = f"<u>{formatted_text}</u>"
            if link_url:
                formatted_text = f"[{formatted_text}]({link_url})"

            color = annotations.get("color")
            if color and color != "default":
                color_map = {
                    "gray": "#787774", "brown": "#9f6b53", "orange": "#d9730d",
                    "yellow": "#dfab01", "green": "#0f7b6c", "blue": "#0b6e99",
                    "purple": "#6940a5", "pink": "#ad1a72", "red": "#e03e3e",
                }
                css_color = color_map.get(color, color)
                formatted_text = f'<span style="color: {css_color}">{formatted_text}</span>'

            markdown_parts.append(formatted_text)

        return "".join(markdown_parts)

    @staticmethod
    def get_lookup_table_name(table_name: str, field_name: str) -> str:
        """Generate the option table name for select and multi-select fields."""
        import re
        cleaned_field = re.sub(r'[^\w]', '_', field_name)
        cleaned_field = re.sub(r'_+', '_', cleaned_field)
        cleaned_field = cleaned_field.strip('_').lower()
        if not cleaned_field or cleaned_field[0].isdigit():
            cleaned_field = f"field_{cleaned_field}"
        return f"{table_name}__{cleaned_field}"
