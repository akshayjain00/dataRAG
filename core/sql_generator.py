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

        # Build context for the LLM
        context = build_context_for_llm(ranked_results, schema)

        # Prepare the prompt for the LLM
        system_prompt = "You are an expert SQL generator. Given the following database schema context and a user query, generate a syntactically correct SQL query that answers the user's question. Only output the SQL query, with no explanation or preamble."
        user_prompt = f"Schema Context:\n{context}\n\nUser Query: {user_query}\n\nSQL Query:"

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