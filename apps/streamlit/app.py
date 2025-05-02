import sys
from pathlib import Path
# Ensure project root is on Python path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st
import os
from core.schema_parser import SchemaParser
from core.sql_generator import SQLGenerator
from core.ingest_utils import load_folder_schema
from core.graph_builder import GraphBuilder
from core.hybrid_retriever import hybrid_retrieve
from pathlib import Path
from core.vector_store_utils import chunk_schema_elements, generate_embeddings, store_embeddings_in_chroma
import numpy as np

# Determine project root regardless of working directory
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # two levels up from apps/streamlit

# insert session state init
if 'schema' not in st.session_state:
    st.session_state['schema'] = None

st.title("Database Schema RAG System")

# Sidebar: Automatic Folder Load
if st.sidebar.button("Load PES docs from ingest folder"):
    # Load all DBT schema definitions in the docs folder
    folder = PROJECT_ROOT / "ingest" / "DE-DBT-SNOWFLAKE" / "models" / "core" / "docs"
    loaded = load_folder_schema(str(folder))
    st.session_state['schema'] = loaded
    if loaded.tables:
        st.sidebar.success(f"Loaded {len(loaded.tables)} tables from docs")
        st.sidebar.write("Tables:", list(loaded.tables.keys()))
    else:
        st.sidebar.error("No valid schema files found in folder.")

# Sidebar: Schema Upload
uploaded_schema = st.sidebar.file_uploader("Upload your database schema (JSON/YAML)", type=["json", "yaml"])

if uploaded_schema:
    st.sidebar.success("Schema uploaded successfully")
    schema_content = uploaded_schema.read().decode("utf-8")
    # Parse Schema
    try:
        parser = SchemaParser()
        parsed = parser.parse(schema_content)
        st.session_state['schema'] = parsed
        st.sidebar.write("Parsed Tables:", list(parsed.tables.keys()))
    except Exception as e:
        st.sidebar.error(f"Error parsing schema: {e}")

# Sidebar: RAGxplorer Mode toggle
if 'ragxplorer_mode' not in st.session_state:
    st.session_state['ragxplorer_mode'] = False
ragxplorer_mode = st.sidebar.toggle("Enable RAGxplorer Mode", value=st.session_state['ragxplorer_mode'])
st.session_state['ragxplorer_mode'] = ragxplorer_mode

# Main Query Input
schema = st.session_state['schema']

# Graph visualization and Ingestion
if schema and schema.tables:
    if st.sidebar.button("Visualize Schema Graph"):
        dot = GraphBuilder.schema_to_dot(schema)
        st.subheader("Schema Graph")
        st.graphviz_chart(dot)

    # Add Neo4j Ingestion Button
    if st.sidebar.button("Ingest Schema to Neo4j"):
        try:
            with st.spinner("Ingesting schema into Neo4j..."):
                GraphBuilder.ingest_schema_to_neo4j(schema)
            st.sidebar.success("Schema ingested into Neo4j successfully!")
        except Exception as e:
            st.sidebar.error(f"Neo4j ingestion failed: {e}")

# After schema is loaded/uploaded and parsed:
if st.session_state['schema']:
    schema = st.session_state['schema']
    st.sidebar.info("Chunking schema and generating embeddings...")
    chunks = chunk_schema_elements(schema)
    chunks = generate_embeddings(chunks)
    store_embeddings_in_chroma(chunks)
    st.sidebar.success("Schema embeddings generated and stored.")

# Question section: only active when a schema is loaded
st.header("Ask a question about your database")
if not schema or not schema.tables:
    st.info("Please load or upload a schema to start asking questions.")
    disabled = True
else:
    disabled = False
user_query = st.text_input("Enter your natural language query:", disabled=disabled)
if user_query and not disabled:
    generator = SQLGenerator()
    with st.spinner("Retrieving context and generating SQL..."):
        # Call hybrid retriever
        ranked_results, diagnostics = hybrid_retrieve(user_query, schema)

        # Generate SQL using LLM with retrieved context
        sql = generator.generate_sql(user_query, ranked_results, schema)

        st.subheader("Generated SQL")
        st.code(sql, language="sql")

        # Display Diagnostics
        with st.expander("Show Retrieval Diagnostics"):
            if ragxplorer_mode:
                st.markdown("### RAGxplorer Diagnostics Table")
                import pandas as pd
                # Prepare table for final ranking
                table_data = []
                for cand in diagnostics['final_ranking']:
                    table_data.append({
                        'Name': cand.get('name'),
                        'Type': cand.get('type'),
                        'Text': cand.get('text'),
                        'Vector Score': round(cand.get('vector_score', 0), 3),
                        'Graph Score': round(cand.get('graph_score', 0), 3),
                        'Keyword Score': round(cand.get('keyword_score', 0), 3),
                        'Final Score': round(cand.get('final_score', 0), 3)
                    })
                df = pd.DataFrame(table_data)
                st.dataframe(df, use_container_width=True)
                # Expandable details for each candidate
                for i, cand in enumerate(diagnostics['final_ranking']):
                    with st.expander(f"Details for: {cand.get('name')}"):
                        st.write("**Type:**", cand.get('type'))
                        st.write("**Text:**", cand.get('text'))
                        st.write("**Vector Score:**", cand.get('vector_score', 0))
                        st.write("**Graph Score:**", cand.get('graph_score', 0))
                        st.write("**Keyword Score:**", cand.get('keyword_score', 0))
                        st.write("**Final Score:**", cand.get('final_score', 0))
                        st.write("**Trace Log:**", cand.get('trace_log', {}))
                st.markdown("---")
                st.markdown("#### Raw Retrieval Results")
                st.write("**Vector Results:**", diagnostics['vector_results'])
                st.write("**Graph Results:**", diagnostics['graph_results'])
                st.write("**Keyword Results:**", diagnostics['keyword_results'])
            else:
                st.json(diagnostics)