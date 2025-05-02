"""Utilities for interacting with the Neo4j graph database."""

import os
from neo4j import GraphDatabase, Driver
from dotenv import load_dotenv
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password") # Replace with your actual default or raise error

_driver: Optional[Driver] = None

def get_driver() -> Driver:
    """Initializes and returns the Neo4j driver instance."""
    global _driver
    if _driver is None:
        logger.info(f"Connecting to Neo4j at {NEO4J_URI}")
        try:
            _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
            _driver.verify_connectivity()
            logger.info("Neo4j connection successful.")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    return _driver

def close_driver():
    """Closes the Neo4j driver connection if it's open."""
    global _driver
    if _driver is not None:
        logger.info("Closing Neo4j connection.")
        _driver.close()
        _driver = None

def run_query(query: str, parameters: Optional[dict] = None):
    """Executes a Cypher query against the Neo4j database."""
    driver = get_driver()
    with driver.session() as session:
        try:
            result = session.run(query, parameters)
            return result.data() # Or handle as needed, e.g., result.consume() for writes
        except Exception as e:
            logger.error(f"Error executing Cypher query: {e}\nQuery: {query}\nParams: {parameters}")
            raise

# Example usage (optional, for testing)
if __name__ == "__main__":
    try:
        # Test connection
        driver = get_driver()
        print("Driver obtained.")

        # Example write query
        # run_query("MERGE (t:TestNode {name: $name}) RETURN t", {"name": "Test"})
        # print("Test node merged.")

        # Example read query
        # result = run_query("MATCH (n) RETURN count(n) as count")
        # print(f"Node count: {result[0]['count']}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        close_driver()
