import pytest
from core.schema_parser import SchemaParser

VALID_SCHEMA = '''
 tables:
   - name: t1
     columns:
       - name: c1
         type: int
         primary_key: true
       - name: c2
         type: int
     foreign_keys:
       - column: c2
         references:
           table: t1
           column: c1
'''

MISSING_PK_SCHEMA = '''
 tables:
   - name: t1
     columns:
       - name: c1
         type: int
     primary_key: c2
'''

MISSING_FK_TABLE = '''
 tables:
   - name: t1
     columns:
       - name: c1
         type: int
       - name: c2
         type: int
     foreign_keys:
       - column: c2
         references:
           table: t2
           column: c3
'''

MISSING_FK_COLUMN = '''
 tables:
   - name: t1
     columns:
       - name: c1
         type: int
     foreign_keys:
       - column: c1
         references:
           table: t1
           column: c2
'''

def test_valid_schema_parses():
    parser = SchemaParser()
    schema = parser.parse(VALID_SCHEMA)
    assert 't1' in schema.tables
    assert schema.tables['t1'].primary_key == 'c1'
    assert 'c1' in schema.tables['t1'].columns

def test_missing_pk_raises():
    parser = SchemaParser()
    with pytest.raises(ValueError) as e:
        parser.parse(MISSING_PK_SCHEMA)
    assert "Primary key 'c2' not found in table 't1'" in str(e.value)

def test_missing_fk_table_raises():
    parser = SchemaParser()
    with pytest.raises(ValueError) as e:
        parser.parse(MISSING_FK_TABLE)
    assert "Referenced table 't2' not found for foreign key on 't1.c2'" in str(e.value)

def test_missing_fk_column_raises():
    parser = SchemaParser()
    with pytest.raises(ValueError) as e:
        parser.parse(MISSING_FK_COLUMN)
    assert "Referenced column 'c2' not found in table 't1' for foreign key on 't1.c1'" in str(e.value) 