import json
import os

from neo4j import GraphDatabase
from qwen_agent.tools.base import register_tool, BaseTool


@register_tool('execute_cypher', allow_overwrite=True)
class ExecuteCypherTool(BaseTool):
    def __init__(self, timeout: int = 60 * 5) -> None:
        neo4j_username = os.environ.get("NEO4J_USER", "neo4j")
        neo4j_password = os.environ.get("NEO4J_PASSWORD")
        self.driver = GraphDatabase.driver('neo4j://localhost:7687', auth=(neo4j_username, neo4j_password))

    name = "execute_cypher"

    description = """
    Execute a Cypher query in Neo4j and return all records as a list of dicts.
    """
    parameters = [{
        "name": "cypher",
        "type": "string",
        "description": "The Cypher query to execute",
        "required": True
    }]

    # async \
    def call(self, params: str, **kwargs):
        cypher = json.loads(params)['cypher']
        results = self.execute(cypher)
        return results

    def execute(self, cypher: str):
        print(f"cypher： {cypher}")
        with self.driver.session() as session:
            result = session.run(cypher)
            records = [record.data() for record in result]
        return records


@register_tool('query_cypher_embedding', allow_overwrite=True)
class QueryCypherEmbeddingTool(BaseTool):
    def __init__(self, timeout: int = 60 * 5) -> None:
        neo4j_username = os.environ.get("NEO4J_USER", "neo4j")
        neo4j_password = os.environ.get("NEO4J_PASSWORD")
        self.driver = GraphDatabase.driver('neo4j://localhost:7687', auth=(neo4j_username, neo4j_password))

    name = "query_cypher_embedding"

    description = """
    Execute a Cypher query in Neo4j with embedding parameters and return records.
    """
    parameters = [{
        "name": "cypher",
        "type": "string",
        "description": "Cypher to execute",
        "required": True
    }, {
        "name": "embedding",
        "type": "object",
        "description": "Cypher parameters (dict), e.g. {'embedding': [...]}",
        "required": False
    }]

    def call(self, params: str, **kwargs):
        payload = json.loads(params)
        cypher = payload["cypher"]
        cypher_params =  payload.get("embedding")  # 保持兼容
        return self.execute(cypher, cypher_params)

    def execute(self, cypher: str, cypher_params=None):
        print(f"cypher: {cypher}")
        with self.driver.session() as session:
            result = session.run(cypher, cypher_params or {})
            return [record.data() for record in result]