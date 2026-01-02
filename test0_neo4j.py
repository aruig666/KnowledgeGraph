
from utils.neo4j import connect_neo4j


if __name__ == "__main__":
    init = False
    driver = connect_neo4j(init=init)
    with driver.session() as session:
        result = session.run("MATCH (n) RETURN count(n) AS node_count")
        record = result.single()
        node_count = record["node_count"] if record else 0
        print(f"Total nodes in the database: {node_count}")
        result = session.run("SHOW CONSTRAINTS YIELD  name;")
        constraints = [record["name"] for record in result]
        print("Constraints in the database:",constraints)
        for name in constraints:
            session.run(f"DROP CONSTRAINT `{name}`")
        result = session.run("SHOW CONSTRAINTS YIELD  name;")
        constraints = [record["name"] for record in result]
        print("Constraints in the database:",constraints)
        result = session.run("SHOW INDEXES YIELD  name;")

        indexs = [record["name"] for record in result]
        print("indexs in the database:",indexs)
        for name in indexs:
            session.run(f"DROP INDEX  `{name}`")
        result = session.run("SHOW INDEXES YIELD  name;")
        indexs = [record["name"] for record in result]
        print("indexs in the database:",indexs)
    driver.close()