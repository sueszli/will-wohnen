from neo4j import GraphDatabase


def create_person(tx, name):
    tx.run("CREATE (a:Person {name: $name})", name=name)


def print_people(tx):
    result = tx.run("MATCH (a:Person) RETURN a.name AS name")
    for record in result:
        print(record["name"])


url = "bolt://main:7687"
user = "neo4j"
password = "password"
driver = GraphDatabase.driver(url, auth=(user, password))

with driver.session() as session:
    session.write_transaction(create_person, "Alice")
    session.write_transaction(create_person, "Bob")
    session.read_transaction(print_people)
