from typing import List, Dict, Any
from core.schema_parser import Schema, Table, Column
import tiktoken

# Use tiktoken for OpenAI models
def count_tokens(text: str, model: str = "gpt-3.5-turbo") -> int:
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        print(f"Warning: Model {model} not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")
    return len(encoding.encode(text))

def format_table_ddl(table: Table) -> str:
    """Generates a simplified DDL string for a table."""
    lines = [f"TABLE {table.name} ("]
    for col_name, col in table.columns.items():
        line = f"    {col_name} {col.data_type}"
        if col.is_primary:
            line += " PRIMARY KEY"
        if col.is_unique:
            line += " UNIQUE"
        lines.append(line + ",")
    # Add FK constraints at the end
    for fk_col, fk in table.foreign_keys.items():
        lines.append(f"    FOREIGN KEY ({fk_col}) REFERENCES {fk.ref_table}({fk.ref_column}),")
    if lines[-1].endswith(","):
         lines[-1] = lines[-1][:-1] # Remove trailing comma
    lines.append(");")
    return "\n".join(lines)

def build_context_for_llm(
    ranked_results: List[Dict[str, Any]],
    schema: Schema,
    max_tokens: int = 3000 # Example limit
) -> str:
    """
    Assembles the most relevant schema context for an LLM based on retriever results.
    Prioritizes higher-ranked tables/columns and their direct relationships.
    """
    context_parts = []
    current_tokens = 0
    included_tables = set()

    prompt_prefix = "Relevant Database Schema:\n"
    current_tokens += count_tokens(prompt_prefix)

    # Iterate through ranked results, prioritizing tables
    for item in ranked_results:
        table_name = item.get('metadata', {}).get('table')
        if not table_name or table_name not in schema.tables:
            continue

        if table_name not in included_tables:
            table = schema.tables[table_name]
            ddl = format_table_ddl(table)
            ddl_tokens = count_tokens(ddl)

            if current_tokens + ddl_tokens <= max_tokens:
                context_parts.append(ddl)
                current_tokens += ddl_tokens
                included_tables.add(table_name)

                # Also consider adding directly related tables via FK if space permits
                related_tables_to_add = set()
                for fk in table.foreign_keys.values():
                    if fk.ref_table not in included_tables and fk.ref_table in schema.tables:
                         related_tables_to_add.add(fk.ref_table)
                # Add incoming FKs?
                # This requires iterating all tables, might be complex/costly here

                for rel_table_name in related_tables_to_add:
                    if rel_table_name not in included_tables: # Double check
                        rel_table = schema.tables[rel_table_name]
                        rel_ddl = format_table_ddl(rel_table)
                        rel_tokens = count_tokens(rel_ddl)
                        if current_tokens + rel_tokens <= max_tokens:
                            context_parts.append(rel_ddl)
                            current_tokens += rel_tokens
                            included_tables.add(rel_table_name)
                        else:
                            break # Stop adding related tables if over limit
            else:
                 # If even the first DDL is too large, maybe add just table name?
                 # Or break if we assume ranked results are most important
                 break # Stop adding tables if over limit

        # If already over limit, stop processing results
        if current_tokens >= max_tokens:
            break

    return prompt_prefix + "\n\n".join(context_parts) 