from typing import List, Dict, Any
from core.schema_parser import Schema, Table, Column
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings

# Initialize embedding model (can be swapped for OpenAI if needed)
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"

# Define a chunk structure
def chunk_schema_elements(schema: Schema) -> List[Dict[str, Any]]:
    """
    Chunk schema into elements suitable for embedding.
    Returns a list of dicts: { 'text': ..., 'type': ..., 'name': ..., 'metadata': ... }
    """
    chunks = []
    # Table-level chunks
    for table_name, table in schema.tables.items():
        chunks.append({
            'text': f"Table: {table_name}",
            'type': 'table',
            'name': table_name,
            'metadata': {'table': table_name}
        })
        # Column-level chunks
        for col_name, col in table.columns.items():
            col_text = f"Column: {col_name} in table {table_name}, type: {col.data_type}"
            if col.is_primary:
                col_text += ", PRIMARY KEY"
            if col.is_unique:
                col_text += ", UNIQUE"
            if col.foreign_key:
                col_text += f", FK to {col.foreign_key.ref_table}({col.foreign_key.ref_column})"
            chunks.append({
                'text': col_text,
                'type': 'column',
                'name': f"{table_name}.{col_name}",
                'metadata': {'table': table_name, 'column': col_name}
            })
        # Relationship-level chunks (FKs)
        for fk in table.foreign_keys.values():
            rel_text = f"Relationship: {table_name}.{fk.column} -> {fk.ref_table}.{fk.ref_column}"
            chunks.append({
                'text': rel_text,
                'type': 'graph_node',
                'name': f"{table_name}.{fk.column}->{fk.ref_table}.{fk.ref_column}",
                'metadata': {
                    'from_table': table_name,
                    'from_column': fk.column,
                    'to_table': fk.ref_table,
                    'to_column': fk.ref_column
                }
            })
    return chunks

# Generate embeddings for schema chunks
def generate_embeddings(chunks: List[Dict[str, Any]], model_name: str = EMBEDDING_MODEL_NAME):
    model = SentenceTransformer(model_name)
    texts = [chunk['text'] for chunk in chunks]
    embeddings = model.encode(texts, show_progress_bar=True)
    for chunk, emb in zip(chunks, embeddings):
        chunk['embedding'] = emb.tolist()
    return chunks

# Store embeddings in Chroma DB
def store_embeddings_in_chroma(chunks: List[Dict[str, Any]], collection_name: str = "schema_chunks"):
    client = chromadb.Client(Settings())
    collection = client.get_or_create_collection(collection_name)
    for idx, chunk in enumerate(chunks):
        collection.add(
            ids=[f"chunk_{idx}"],
            embeddings=[chunk['embedding']],
            metadatas=[{
                **chunk['metadata'],
                'type': chunk['type'],
                'name': chunk['name']
            }],
            documents=[chunk['text']]
        )
    return collection

# Helper: Semantic search in Chroma
def semantic_search(query, collection_name="schema_chunks", top_k=5):
    client = chromadb.Client(Settings())
    collection = client.get_or_create_collection(collection_name)
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    query_emb = model.encode([query])[0]
    results = collection.query(
        query_embeddings=[query_emb.tolist()],
        n_results=top_k,
        include=["metadatas", "documents", "distances"]
    )
    hits = []
    if results and results.get("documents") and results["documents"][0]:
        for doc, meta, dist in zip(results["documents"][0], results["metadatas"][0], results["distances"][0]):
            hits.append({"text": doc, "metadata": meta, "distance": dist})
    return hits
