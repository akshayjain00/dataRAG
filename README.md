# Database Schema RAG System

This project is a Database Schema RAG (Retrieval Augmented Generation) System that identifies relevant tables and generates SQL queries based on natural language input.

## Core Features
- Schema parsing with relationship awareness (PK/FK).
- Dual ingestion: vector embeddings and knowledge graph.
- Hybrid retrieval combining semantic similarity and graph traversal.
- SQL generation with validation and self-repair.
- Streamlit UI for interactive querying and visualization.

## Directory Structure
```text
/apps
  └── streamlit - Streamlit front-end application
/core - Core library modules for schema ingestion, retrieval, and SQL generation
/ingest - Data ingestion pipelines (vector and graph)
/tests - Automated tests and golden datasets
.github/workflows - CI/CD
```

## Getting Started

Install dependencies:
```
pip install -r requirements.txt
```

Run the Streamlit app:
```
streamlit run apps/streamlit/streamlit_app.py
```

## Contributing

Please follow the contributing guidelines and coding standards outlined in CONTRIBUTING.md.
