import os
import pytest
from core.schema_parser import SchemaParser, Schema, Table, Column, ForeignKey

# Helper to load sample schema path
def get_sample_path():
    return os.path.join(os.path.dirname(__file__), "sample_schema.yaml")


def test_parse_valid_schema_from_yaml():
    schema = SchemaParser.from_yaml(get_sample_path())
    assert isinstance(schema, Schema)
    assert "users" in schema.tables
    assert "posts" in schema.tables

    users_table = schema.tables["users"]
    id_col = users_table.columns["id"]
    assert id_col.is_primary is True
    assert id_col.data_type == "integer"
    assert id_col.foreign_key is None

    posts_table = schema.tables["posts"]
    fk_col = posts_table.columns["user_id"]
    assert fk_col.foreign_key is not None
    fk = fk_col.foreign_key
    assert fk.ref_table == "users"
    assert fk.ref_column == "id"


def test_parse_missing_fk_table_raises():
    bad_schema = {
        "tables": {
            "a": {"columns": {"id": {"type": "integer", "primary_key": True}}, "foreign_keys": []},
            "b": {"columns": {"id": {"type": "integer", "primary_key": True}, "a_id": {"type": "integer"}},
                  "foreign_keys": [{"column": "a_id", "references": {"table": "non_exist", "column": "id"}}]}
        }
    }
    with pytest.raises(ValueError) as excinfo:
        SchemaParser.parse(bad_schema)
    assert "Referenced table non_exist not found for foreign key" in str(excinfo.value) 