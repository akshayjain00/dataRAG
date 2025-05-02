import os
from pathlib import Path
from core.schema_parser import SchemaParser, Schema

def load_folder_schema(folder_path: str) -> Schema:
    """
    Load all JSON, YAML, and YML schema files under folder_path,
    parse them, and merge into a single Schema object.
    """
    parser = SchemaParser()
    master_schema = Schema()
    root = Path(folder_path)
    if not root.is_dir():
        # No such folder, return empty schema
        return master_schema
    # Collect all JSON, YAML, and YML files recursively
    files = list(root.rglob("*.json")) + list(root.rglob("*.yaml")) + list(root.rglob("*.yml"))
    for filepath in files:
        try:
            content = Path(filepath).read_text()
            schema = parser.parse(content)
            # Merge tables
            for tbl_name, tbl in schema.tables.items():
                if tbl_name not in master_schema.tables:
                    master_schema.tables[tbl_name] = tbl
                else:
                    # Merge columns
                    existing = master_schema.tables[tbl_name]
                    if hasattr(tbl, "columns"):
                        for col_name, col in tbl.columns.items():
                            if col_name not in existing.columns:
                                existing.columns[col_name] = col
            # Note: primary keys and foreign keys merging can be added later
        except Exception:
            # Skip any files or errors silently
            continue
    return master_schema 