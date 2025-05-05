from typing import List, Dict, Any
from core.schema_parser import Schema, ForeignKey
from core.context_builder import build_context_for_llm
import openai
import os
from dotenv import load_dotenv
from core.neo4j_utils import run_query

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
    ) -> (str, str):
        """
        Generate SQL using an LLM based on the user query and retrieved schema context.
        """
        # Handle empty schema
        if not schema.tables:
            return ("-- No tables available in schema.", "No schema loaded; cannot generate SQL.")

        # Identify top-ranked table from retrieval diagnostics
        top_table = None
        if ranked_results:
            for result in ranked_results:
                if result.get('type') == 'table' and result.get('name') in schema.tables:
                    top_table = schema.tables[result['name']]
                    break
        
        if not top_table:
            return ("-- No relevant table found in retrieval diagnostics.", "No relevant table found; cannot generate SQL.")

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
            warning_sql = f"-- Warning: The following columns are not present in table '{top_table.name}': {', '.join(missing_columns)}.\n-- Available columns: {columns_str}"
            explanation = f"The following columns are not present in table '{top_table.name}': {', '.join(missing_columns)}. Available columns: {columns_str}"
            return warning_sql, explanation

        # Call LLM to generate SQL
        response = openai.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.0,
        )
        sql_query = response.choices[0].message.content.strip()
        # Cleanup markdown fences
        if sql_query.startswith("```sql"):
            sql_query = sql_query[5:].strip()
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3].strip()
        # Self-repair: validate SQL syntax using SQLAlchemy
        from sqlalchemy import text
        try:
            text(sql_query)
        except Exception as err:
            # Ask LLM to fix the SQL
            repair_prompt = (
                f"The following SQL resulted in an error: {err}."
                f" Please provide a corrected SQL statement without explanation."
                f" SQL to fix: {sql_query}"
            )
            repair_resp = openai.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": "You are an SQL repair assistant."},
                    {"role": "user", "content": repair_prompt}
                ],
                temperature=0.0,
            )
            sql_query = repair_resp.choices[0].message.content.strip()
        # Generate natural-language explanation
        explain_prompt = f"Provide a brief explanation of what this SQL does: {sql_query}"
        explain_resp = openai.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": "You are an SQL explanation assistant."},
                {"role": "user", "content": explain_prompt}
            ],
            temperature=0.0,
        )
        explanation = explain_resp.choices[0].message.content.strip()
        return sql_query, explanation

    def _find_join_path_graph(self, start: str, end: str) -> List[str]:
        """
        Finds the shortest path of table nodes between start and end in Neo4j via HAS_COLUMN and FOREIGN_KEY edges.
        """
        cypher = """
        MATCH p = shortestPath(
          (tStart:Table {name: $start})-[:HAS_COLUMN|FOREIGN_KEY*]-(tEnd:Table {name: $end})
        )
        RETURN [n IN nodes(p) WHERE 'Table' IN labels(n) | n.name] AS tables LIMIT 1
        """
        try:
            result = run_query(cypher, {'start': start, 'end': end})
            if result and 'tables' in result[0]:
                return result[0]['tables']
        except Exception:
            pass
        return []

    def _find_join_path(self, start: str, end: str, schema: Schema) -> List[str]:
        """
        Finds the shortest path (as a list of table names) between start and end using FK relationships.
        """
        from collections import deque
        visited = set([start])
        queue = deque([[start]])
        while queue:
            path = queue.popleft()
            node = path[-1]
            if node == end:
                return path
            neighbors = []
            # Outbound FKs
            for col in schema.tables[node].columns.values():
                fk = col.foreign_key
                if fk:
                    neighbors.append(fk.ref_table)
            # Inbound FKs
            for tbl_name, tbl in schema.tables.items():
                for col in tbl.columns.values():
                    if col.foreign_key and col.foreign_key.ref_table == node:
                        neighbors.append(tbl_name)
            for nb in set(neighbors):
                if nb not in visited:
                    visited.add(nb)
                    queue.append(path + [nb])
        return []

    def generate_join_sql(self, target_tables: List[str], schema: Schema) -> str:
        """
        Generate a multi-table JOIN SQL by discovering join paths via FK relationships in-memory.
        """
        if not target_tables:
            return "-- No tables specified for join generation."
        base = target_tables[0]
        sql = f"SELECT * FROM {base}"
        for tbl in target_tables[1:]:
            # Use graph DB to find table path
            path = self._find_join_path_graph(base, tbl)
            if not path:
                # Fallback to in-memory path
                path = self._find_join_path(base, tbl, schema)
            if not path:
                sql += f"\n-- No join path found between {base} and {tbl}"  
                continue
            # Build join clauses along the path
            for i in range(1, len(path)):
                left = path[i-1]
                right = path[i]
                join_clause = None
                # Try fk from left to right
                for col in schema.tables[left].columns.values():
                    fk = col.foreign_key
                    if fk and fk.ref_table == right:
                        join_clause = f"JOIN {right} ON {left}.{col.name} = {right}.{fk.ref_column}"
                        break
                # Try fk from right to left
                if not join_clause:
                    for col in schema.tables[right].columns.values():
                        fk = col.foreign_key
                        if fk and fk.ref_table == left:
                            join_clause = f"JOIN {right} ON {right}.{col.name} = {left}.{fk.ref_column}"
                            break
                if join_clause:
                    sql += f" \n{join_clause}"
                else:
                    sql += f"\n-- No join clause found for {left} <-> {right}"
        return sql + ";"

    # Add a CTE-based join method to wrap the join SQL in a common table expression
    def generate_cte_join_sql(self, target_tables: List[str], schema: Schema) -> str:
        """
        Generate a multi-table JOIN SQL wrapped in a common table expression (CTE) for the given target tables.
        """
        # Generate the base join SQL and strip trailing semicolon
        base_sql = self.generate_join_sql(target_tables, schema).rstrip(';')
        # Create a CTE name from the joined tables
        cte_name = "_".join(target_tables) if target_tables else "joined_data"
        # Build the CTE wrapper
        cte_sql = (
            f"WITH {cte_name} AS (\n"
            f"    {base_sql}\n"
            ")\n"
            f"SELECT * FROM {cte_name};"
        )
        return cte_sql