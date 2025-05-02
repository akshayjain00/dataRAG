from core.schema_parser import Schema, Table, Column, ForeignKey
from core.neo4j_utils import run_query, close_driver # Import Neo4j utilities
import logging

logger = logging.getLogger(__name__)

class GraphBuilder:
    @staticmethod
    def schema_to_dot(schema: Schema) -> str:
        """
        Produce a GraphViz DOT string representing tables and foreign key relationships.
        """
        lines = ["digraph schema {", "    rankdir=LR;", "    node [shape=box];"]
        # Add table nodes
        for table_name, table in schema.tables.items():
            lines.append(f'    "{table_name}";')
        # Add edges for foreign keys
        for table_name, table in schema.tables.items():
            for fk in table.foreign_keys.values():
                # Edge from table to referenced table
                lines.append(
                    f'    "{table_name}" -> "{fk.ref_table}" [label="{fk.column}"];'
                )
        lines.append("}")
        return "\n".join(lines)

    @staticmethod
    def ingest_schema_to_neo4j(schema: Schema):
        """
        Ingests the parsed schema (tables, columns, FKs) into Neo4j.
        Uses MERGE to avoid duplicates.
        """
        logger.info(f"Starting Neo4j ingestion for {len(schema.tables)} tables.")
        try:
            # Optional: Add constraints for uniqueness and faster lookups
            run_query("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Table) REQUIRE t.name IS UNIQUE")
            run_query("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Column) REQUIRE c.id IS UNIQUE")

            # Ingest Tables and Columns
            for table_name, table in schema.tables.items():
                # Create Table node
                run_query(
                    "MERGE (t:Table {name: $table_name}) RETURN t",
                    {"table_name": table_name}
                )
                logger.debug(f"Merged Table node: {table_name}")

                for col_name, column in table.columns.items():
                    # Create a unique ID for the column node
                    col_id = f"{table_name}.{col_name}"
                    # Create Column node
                    run_query(
                        "MERGE (c:Column {id: $col_id}) SET c.name = $col_name, c.type = $col_type RETURN c",
                        {"col_id": col_id, "col_name": col_name, "col_type": column.data_type}
                    )
                    logger.debug(f"Merged Column node: {col_id}")

                    # Create relationship from Table to Column
                    run_query(
                        """
                        MATCH (t:Table {name: $table_name})
                        MATCH (c:Column {id: $col_id})
                        MERGE (t)-[:HAS_COLUMN]->(c)
                        """,
                        {"table_name": table_name, "col_id": col_id}
                    )
                    logger.debug(f"Merged HAS_COLUMN relationship: {table_name} -> {col_id}")

            # Ingest Foreign Keys (after all tables/columns exist)
            logger.info("Ingesting foreign key relationships...")
            for table_name, table in schema.tables.items():
                for col_name, column in table.columns.items():
                    if column.foreign_key:
                        fk = column.foreign_key
                        source_col_id = f"{table_name}.{col_name}"
                        target_col_id = f"{fk.ref_table}.{fk.ref_column}"

                        # Create FOREIGN_KEY relationship
                        run_query(
                            """
                            MATCH (source:Column {id: $source_col_id})
                            MATCH (target:Column {id: $target_col_id})
                            MERGE (source)-[:FOREIGN_KEY]->(target)
                            """,
                            {"source_col_id": source_col_id, "target_col_id": target_col_id}
                        )
                        logger.debug(f"Merged FOREIGN_KEY relationship: {source_col_id} -> {target_col_id}")

            logger.info("Neo4j ingestion completed successfully.")

        except Exception as e:
            logger.error(f"Error during Neo4j ingestion: {e}")
            # Optionally re-raise or handle specific exceptions
            raise
        # Note: Driver closure is handled by neo4j_utils, typically at app shutdown
        # or you could explicitly call close_driver() if running as a standalone script.