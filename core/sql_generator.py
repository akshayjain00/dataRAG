from typing import List, Dict, Any
from core.schema_parser import Schema, ForeignKey
from core.context_builder import build_context_for_llm
import openai
import os
from dotenv import load_dotenv

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# Define the default LLM model
LLM_MODEL = "gpt-3.5-turbo"

class SQLGenerator:
    def generate_sql(
        self,
        user_query: str,
        ranked_results: List[Dict[str, Any]],
        schema: Schema
    ) -> str:
        """
        Generate SQL using an LLM based on the user query and retrieved schema context.
        """
        # Handle empty schema
        if not schema.tables:
            return "-- No tables available in schema."

        # Identify top-ranked table from retrieval diagnostics
        top_table = None
        if ranked_results:
            for result in ranked_results:
                if result.get('type') == 'table' and result.get('name') in schema.tables:
                    top_table = schema.tables[result['name']]
                    break
        
        if not top_table:
            return "-- No relevant table found in retrieval diagnostics."

        # Gather columns for the top table
        table_columns = list(top_table.columns.keys())
        columns_str = ', '.join(table_columns)

        # Optionally, parse user_query for column names (simple heuristic)
        referenced_columns = [col for col in table_columns if col.lower() in user_query.lower()]
        missing_columns = []
        # If user_query references columns not in table, collect them
        for word in user_query.replace(',', ' ').split():
            if word.lower() not in [c.lower() for c in table_columns] and word.isidentifier():
                missing_columns.append(word)
        
        # Build context for the LLM
        context = build_context_for_llm(ranked_results, schema)
        
        # Add explicit table/column info and instructions to the prompt
        system_prompt = (
            "You are an expert SQL generator. Given the following database schema context and a user query, "
            "generate a syntactically correct SQL query that answers the user's question. "
            f"Always use the table '{top_table.name}' and its columns: {columns_str}. "
            "If the user asks for columns not present in this table, suggest the closest available columns. "
            "Only output the SQL query, with no explanation or preamble."
        )
        user_prompt = f"Schema Context:\n{context}\n\nUser Query: {user_query}\n\nSQL Query:"

        # Warn if columns are missing
        if missing_columns:
            return f"-- Warning: The following columns are not present in table '{top_table.name}': {', '.join(missing_columns)}.\n-- Available columns: {columns_str}"

        try:
            if not openai.api_key:
                return "-- Error: OPENAI_API_KEY not set."

            response = openai.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0, # Deterministic output
            )
            sql_query = response.choices[0].message.content.strip()
            # Basic cleanup - remove potential markdown code blocks
            if sql_query.startswith("```sql"):
                sql_query = sql_query[6:]
            if sql_query.endswith("```"):
                sql_query = sql_query[:-3]
            return sql_query.strip()

        except Exception as e:
            return f"-- Error generating SQL: {e}"