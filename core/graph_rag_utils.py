from typing import List, Tuple
from core.schema_parser import Schema, Table, ForeignKey

def extract_graph_rag_triples(schema: Schema) -> List[Tuple[str, str, str]]:
    """
    Extracts triples for GraphRAG: (subject, predicate, object).
    - Table HAS_COLUMN column
    - Table FOREIGN_KEY referenced_table
    """
    triples: List[Tuple[str, str, str]] = []
    for table_name, table in schema.tables.items():
        # Table->Column edges
        for col_name in table.columns.keys():
            triples.append((table_name, "HAS_COLUMN", col_name))
        # Table->Table foreign key edges
        for fk in table.foreign_keys.values():
            triples.append((table_name, "FOREIGN_KEY", fk.ref_table))
    return triples 

import pytest
from core.graph_rag_utils import extract_graph_rag_triples
from core.schema_parser import SchemaParser

SAMPLE_SCHEMA = '''
models:
  - name: table1
    columns:
      - name: col1
        type: int
      - name: col2
        type: int
    foreign_keys:
      - column: col2
        references:
          table: table2
          column: colA
  - name: table2
    columns:
      - name: colA
        type: varchar
'''

EXPECTED_TRIPLES = [
    ('table1', 'HAS_COLUMN', 'col1'),
    ('table1', 'HAS_COLUMN', 'col2'),
    ('table1', 'FOREIGN_KEY', 'table2'),
    ('table2', 'HAS_COLUMN', 'colA'),
]

def test_extract_graph_rag_triples():
    parser = SchemaParser()
    schema = parser.parse(SAMPLE_SCHEMA)
    triples = extract_graph_rag_triples(schema)
    for et in EXPECTED_TRIPLES:
        assert et in triples
    assert len(triples) == len(EXPECTED_TRIPLES) 