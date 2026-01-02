
import os

from neo4j import GraphDatabase


def connect_neo4j(init=False):
    neo4j_username = os.environ.get("NEO4J_USER", "neo4j")
    neo4j_password = os.environ.get("NEO4J_PASSWORD")

    # 注意，这里的用户名为neo4j全局用户名，而非DBMS或者database的名称
    driver = GraphDatabase.driver('neo4j://localhost:7687', auth=(neo4j_username, neo4j_password))
    if init == True:
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("remove all succeed!")
            result = session.run("SHOW CONSTRAINTS YIELD  name;")
            constraints = [record["name"] for record in result]
            for name in constraints:
                session.run(f"DROP CONSTRAINT {name}")
            print("drop constraints succeed!",len(constraints))
            result = session.run("SHOW INDEXES YIELD  name;")
            indexs = [record["name"] for record in result]
            for name in indexs:
                session.run(f"DROP INDEX  {name}")
            print("drop indexs succeed!",len(indexs))

    # print(driver)
    return driver



if __name__ == "__main__":
    '''
    # test connect neo4j
    init = True
    driver = connect_neo4j(init=init)
    with driver.session() as session:
        None
    driver.close()
    '''
    pass


