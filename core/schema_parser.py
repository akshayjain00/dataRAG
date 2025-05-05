"""Module for parsing database schema definitions from YAML or JSON into an internal model."""

import yaml
import json
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Union
import re

@dataclass
class ForeignKey:
    column: str
    ref_table: str
    ref_column: str

@dataclass
class Column:
    name: str
    data_type: str
    is_primary: bool = False
    is_unique: bool = False
    foreign_key: Optional[ForeignKey] = None

@dataclass
class Table:
    name: str
    columns: Dict[str, Column] = field(default_factory=dict)
    primary_key: Optional[str] = None
    foreign_keys: Dict[str, ForeignKey] = field(default_factory=dict)

@dataclass
class Schema:
    tables: Dict[str, Table] = field(default_factory=dict)

class SchemaParser:
    @staticmethod
    def parse(content: Union[str, dict]) -> Schema:
        # Determine format
        if isinstance(content, dict):
            data = content
        else:
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                try:
                    data = yaml.safe_load(content)
                except (yaml.YAMLError, Exception):
                    return Schema()
            except Exception:
                return Schema()

        tables = {}

        # Support 'tables' as a mapping: { table_name: {columns, primary_key, foreign_keys} }
        raw_tables = data.get("tables")
        if isinstance(raw_tables, dict):
            for name, table_def in raw_tables.items():
                if not name or not isinstance(table_def, dict):
                    continue
                raw_cols = table_def.get("columns", {})
                columns_map = {}
                # support dict-style columns mapping
                if isinstance(raw_cols, dict):
                    col_items = raw_cols.items()
                else:
                    col_items = [(col.get("name"), col) for col in raw_cols if isinstance(col, dict) and col.get("name")]
                # Parse columns
                for col_name, col_def in col_items:
                    if not col_name or not isinstance(col_def, dict):
                        continue
                    data_type = col_def.get("type") or col_def.get("data_type")
                    if not data_type:
                        continue
                    is_pk = col_def.get("primary_key", False) or (table_def.get("primary_key") == col_name)
                    is_unique = col_def.get("unique", False)
                    columns_map[col_name] = Column(
                        name=col_name,
                        data_type=data_type,
                        is_primary=is_pk,
                        is_unique=is_unique
                    )
                # Parse FKs
                foreign_keys_map = {}
                for fk in table_def.get("foreign_keys", []):
                    if not isinstance(fk, dict):
                        continue
                    col_raw = fk.get("column")
                    ref = fk.get("references", {})
                    if not isinstance(ref, dict):
                        continue
                    ref_table_raw = ref.get("table")
                    ref_column_raw = ref.get("column")
                    if col_raw and ref_table_raw and ref_column_raw:
                        # sanitize names: strip whitespace and punctuation
                        col_name = str(col_raw).strip()
                        ref_table_name = re.sub(r'^\W+|\W+$', '', str(ref_table_raw).strip())
                        ref_column_name = re.sub(r'^\W+|\W+$', '', str(ref_column_raw).strip())
                        fk_obj = ForeignKey(column=col_name, ref_table=ref_table_name, ref_column=ref_column_name)
                        if col_name in columns_map:
                            columns_map[col_name].foreign_key = fk_obj
                        foreign_keys_map[col_name] = fk_obj
                tables[name] = Table(name=name, columns=columns_map,
                                     primary_key=table_def.get("primary_key"),
                                     foreign_keys=foreign_keys_map)
        # If mapping style not used, try list-of-dict format
        elif isinstance(raw_tables, list):
            for table_def in raw_tables:
                if not isinstance(table_def, dict):
                    continue
                name = table_def.get("name")
                if not name:
                    continue
                raw_cols = table_def.get("columns", {})
                columns_map = {}
                if isinstance(raw_cols, dict):
                    col_items = raw_cols.items()
                else:
                    col_items = [(col.get("name"), col) for col in raw_cols if isinstance(col, dict) and col.get("name")]
                for col_name, col_def in col_items:
                    if not col_name or not isinstance(col_def, dict):
                        continue
                    data_type = col_def.get("type") or col_def.get("data_type")
                    if not data_type:
                        continue
                    is_pk = col_def.get("primary_key", False) or (table_def.get("primary_key") == col_name)
                    is_unique = col_def.get("unique", False)
                    columns_map[col_name] = Column(
                        name=col_name,
                        data_type=data_type,
                        is_primary=is_pk,
                        is_unique=is_unique
                    )
                fk_defs = table_def.get("foreign_keys", []) or []
                foreign_keys_map = {}
                if isinstance(fk_defs, list):
                    for fk in fk_defs:
                        if not isinstance(fk, dict):
                            continue
                        col = fk.get("column")
                        ref = fk.get("references", {})
                        if not isinstance(ref, dict):
                            continue
                        ref_table = ref.get("table")
                        ref_column = ref.get("column")
                        if col and ref_table and ref_column:
                            fk_obj = ForeignKey(column=col, ref_table=ref_table, ref_column=ref_column)
                            if col in columns_map:
                                columns_map[col].foreign_key = fk_obj
                            foreign_keys_map[col] = fk_obj
                tables[name] = Table(name=name, columns=columns_map,
                                     primary_key=table_def.get("primary_key"),
                                     foreign_keys=foreign_keys_map)
        # Next, try dbt models...
        elif "models" in data and isinstance(data.get("models"), list):
            for model_def in data.get("models", []):
                if not isinstance(model_def, dict): continue # Skip invalid entries
                name = model_def.get("name")
                if not name: continue # Skip models without names

                columns_map = {}
                raw_cols = model_def.get("columns", [])
                if isinstance(raw_cols, list):
                    for col_def in raw_cols:
                        if not isinstance(col_def, dict): continue # Skip invalid column defs
                        col_name = col_def.get("name")
                        data_type = col_def.get("data_type") # dbt uses 'data_type'
                        if not col_name or not data_type: continue # Skip columns without name or type

                        # Basic column parsing from dbt format
                        # Add more sophisticated parsing (tests, constraints) if needed
                        columns_map[col_name] = Column(
                            name=col_name,
                            data_type=data_type,
                            # dbt schema files don't typically define PK/FK directly here
                            # This info might be in constraints or separate model properties
                            is_primary=False,
                            is_unique=False
                        )

                # Create table if columns were found
                if columns_map:
                     tables[name] = Table(
                         name=name,
                         columns=columns_map
                         # PK/FK info usually not in this part of dbt schema.yml
                     )
        # Add similar logic for 'sources' if needed

        # Fallback: parse RAG docs examples to extract table names if no tables loaded
        if not tables and isinstance(data, dict) and "examples" in data and isinstance(data["examples"], list):
            for example in data["examples"]:
                if not isinstance(example, dict):
                    continue
                for tbl in example.get("tables", []):
                    if tbl and tbl not in tables:
                        # Create stub Table with no column details
                        tables[tbl] = Table(name=tbl)

        # Validate schema integrity: primary keys and foreign key references
        for table in tables.values():
            if table.primary_key and table.primary_key not in table.columns:
                raise ValueError(f"Primary key {table.primary_key} not found in table {table.name}")
            for fk in table.foreign_keys.values():
                if fk.ref_table not in tables:
                    raise ValueError(f"Referenced table {fk.ref_table} not found for foreign key on {table.name}.{fk.column}")
                if fk.ref_column not in tables[fk.ref_table].columns:
                    raise ValueError(f"Referenced column {fk.ref_column} not found in table {fk.ref_table} for foreign key on {table.name}.{fk.column}")
        return Schema(tables)

    @staticmethod
    def from_yaml(path: str) -> Schema:
        try:
            with open(path, "r") as f:
                content = f.read()
            parser = SchemaParser()
            return parser.parse(content)
        except Exception:
            # Handle file reading or parsing errors
            return Schema()

    @staticmethod
    def from_json(path: str) -> Schema:
        try:
            with open(path, "r") as f:
                content = f.read()
            parser = SchemaParser()
            return parser.parse(content)
        except Exception:
            # Handle file reading or parsing errors
            return Schema()