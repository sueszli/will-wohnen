import csv
import glob
from glob import glob
from pathlib import Path

from neo4j import GraphDatabase

uri = "bolt://main:7687"
auth = ("neo4j", "password")
driver = GraphDatabase.driver(uri, auth=auth)
session = driver.session(database="neo4j")
session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))  # wipe

inputpath = glob.glob(str(Path.cwd() / "data" / "pages_*.csv"))
inputpath = list(filter(lambda p: Path(p).name.startswith("pages_") and Path(p).name.endswith(".csv"), inputpath))
assert len(inputpath) > 0
inputpath.sort()
inputpath = inputpath[-1]
inputfile = list(csv.reader(open(inputpath, "r")))
header = [word for word in inputfile[0]]
body = inputfile[1:]
dicts = list(map(lambda row: dict(zip(header, row)), body))  # convert to dict
dicts = list(map(lambda elem: {k: (v if v != "" else None) for k, v in elem.items()}, dicts))  # get null

# property features:
#     "address": "1200 wien, 20. bezirk, brigittenau, jägerstraße",
#     "bautyp": "altbau",
#     "boeden": "parkett",
#     "energy_certificate": "d",
#     "price": "229000.0",
#     "url": "https://www.willhaben.at/iad/immobilien/d/eigentumswohnung/wien/wien-1200-brigittenau/-wow-die-perfe...",
#     "objekttyp": "wohnung",
#     "stockwerke": "3",
#     "title": "++ wow ++ die perfekte stadtwohnung + aufwendige sanierung + balkon im innenhof + inkl. küche",
#     "total_additional_costs": "5759.43",
#     "type": "balkon",
#     "verfuegbar": "2027",
#     "wohnflaeche": "38.05",
#     "zimmer": "2.0",
#     "zustand": "renoviert"
#     "heizung": "etagenheizung",

# company features:
#     "company_address": "messendorferstraße 71a8041 graz",
#     "company_name": "schantl ith immobilientreuhand gmbh",
#     "company_reference_id": "267143",
#     "company_url": "http://www.schantl-ith.at",

# broker:
#     "company_broker_name": "magdalena tiatco-frank",


# agreement:
#     "last_update": "26.08.2024 12:00",
#     "maklerprovision": "6870.0",


for elem in dicts:
    pass


# def create_nodes(tx):
#     tx.run("CREATE (a:Person {name: 'Alice'})")
#     tx.run("CREATE (a:Person {name: 'Bob'})")
#     tx.run("CREATE (a:Person {name: 'Charlie'})")

# session.execute_write(create_nodes)

# def create_relationships(tx):
#     tx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)")
#     tx.run("MATCH (a:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)")

# session.execute_write(create_relationships)

# def get_nodes(tx):
#     result = tx.run("MATCH (a:Person) RETURN a.name AS name")
#     for record in result:
#         print(record["name"])

# session.execute_read(get_nodes)

# def get_relationships(tx):
#     result = tx.run("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS source, b.name AS target")
#     for record in result:
#         print(f"{record['source']} -> {record['target']}")

# session.execute_read(get_relationships)


session.close()
driver.close()
