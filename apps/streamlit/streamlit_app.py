import streamlit as st
import json
import yaml

st.title("Database Schema RAG System")

st.sidebar.header("Upload Database Schema")
uploaded_file = st.sidebar.file_uploader("Upload your schema JSON or YAML file", type=["json", "yaml", "yml"])
if uploaded_file:
    st.write("Schema file uploaded:", uploaded_file.name)
    try:
        if uploaded_file.name.lower().endswith((".yaml", ".yml")):
            schema = yaml.safe_load(uploaded_file)
        else:
            schema = json.load(uploaded_file)
        st.subheader("Loaded Schema")
        st.write(schema)
        tables = list(schema.get("tables", {}).keys())
        if tables:
            selected_tables = st.multiselect("Select tables to include", tables)
            if selected_tables:
                st.write("Selected tables:", selected_tables)
                # TODO: generate SQL for selected tables
    except Exception as e:
        st.error(f"Failed to parse schema: {e}")
from core.schema_parser import SchemaParser
from core.sql_generator import SQLGenerator
