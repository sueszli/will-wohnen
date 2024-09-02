from neo4j import GraphDatabase

url = "bolt://172.18.0.2:7687"
user = "neo4j"
password = "password"

driver = GraphDatabase.driver(url, auth=(user, password))

def create_person(tx, name):
    tx.run("CREATE (a:Person {name: $name})", name=name)

def print_person(tx, name):
    for record in tx.run("MATCH (a:Person) WHERE a.name = $name RETURN a.name", name=name):
        print(record["a.name"])

with driver.session() as session:
    session.execute_write(create_person, "Alice")
    session.execute_read(print_person, "Alice")

driver.close()
