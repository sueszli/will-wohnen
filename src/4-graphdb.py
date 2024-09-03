import csv
import glob
from glob import glob
from pathlib import Path

from neo4j import GraphDatabase
from tqdm import tqdm


def show_nodes(tx):
    result = tx.run("MATCH (n) RETURN n")
    for record in result:
        print(record)


def show_edges(tx):
    result = tx.run("MATCH ()-[r]->() RETURN r")
    for record in result:
        print(record)


def get_property(tx, url: str) -> dict:
    result = tx.run("MATCH (p:Property {url: $url}) RETURN p", url=url)
    return result.single().value()


def get_company(tx, company_reference_id: str) -> dict:
    result = tx.run("MATCH (c:Company {company_reference_id: $company_reference_id}) RETURN c", company_reference_id=company_reference_id)
    return result.single().value()


def get_broker(tx, company_broker_name: str) -> dict:
    result = tx.run("MATCH (b:Broker {company_broker_name: $company_broker_name}) RETURN b", company_broker_name=company_broker_name)
    return result.single().value()


def get_employed_by(tx, company_broker_name: str, company_url: str) -> dict:
    result = tx.run("MATCH (b:Broker {company_broker_name: $company_broker_name})-[r:employed_by]->(c:Company {company_url: $company_url}) RETURN r", company_broker_name=company_broker_name, company_url=company_url)
    record = result.single()
    return record.value() if record else None


def get_manages(tx, company_broker_name: str, url: str) -> dict:
    result = tx.run(
        """
        MATCH (b:Broker {company_broker_name: $company_broker_name})-[r:manages]->(p:Property {url: $url})
        RETURN r {
            .last_update,
            maklerprovision: CASE WHEN r.maklerprovision = 'N/A' THEN null ELSE r.maklerprovision END
        }
        """,
        company_broker_name=company_broker_name,
        url=url,
    )
    return result.single().value()


# property:
#     ID: "url": "https://www.willhaben.at/iad/immobilien/d/eigentumswohnung/wien/wien-1200-brigittenau/-wow-die-perfe...",
#     "links_price": "229000.0",
#     "description_price": "5759.43", # any additional one-time costs
#     "links_address": "1200 wien, 20. bezirk, brigittenau, jägerstraße",
#     "bautyp": "altbau",
#     "boeden": "parkett",
#     "energy_certificate": "d",
#     "heizung": "etagenheizung",
#     "links_type": "balkon",
#     "objekttyp": "wohnung",
#     "stockwerke": "3",
#     "verfuegbar": "2027",
#     "wohnflaeche": "38.05",
#     "zimmer": "2.0",
#     "zustand": "renoviert"

# company:
#     ID: "company_reference_id": "267143",
#     "company_url": "http://www.schantl-ith.at",
#     "company_address": "messendorferstraße 71a8041 graz",
#     "company_name": "schantl ith immobilientreuhand gmbh",

# broker:
#     ID: "company_broker_name": "magdalena tiatco-frank",

# broker -- agreement --> property
#     "last_update": "26.08.2024 12:00",
#     "maklerprovision": "6870.0",


uri = "bolt://main:7687"
auth = ("neo4j", "password")
driver = GraphDatabase.driver(uri, auth=auth)
session = driver.session(database="neo4j")
session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))  # wipe

inputpath = glob(str(Path.cwd() / "data" / "pages_*.csv"))
inputpath = list(filter(lambda p: Path(p).name.startswith("pages_") and Path(p).name.endswith(".csv"), inputpath))
assert len(inputpath) > 0
inputpath.sort()
inputpath = inputpath[-1]
inputfile = list(csv.reader(open(inputpath, "r")))
header = [word for word in inputfile[0]]
body = inputfile[1:]
dicts = list(map(lambda row: dict(zip(header, row)), body))  # convert to dict
dicts = list(map(lambda elem: {k: (v if v != "" else None) for k, v in elem.items()}, dicts))  # get null

for elem in tqdm(dicts):

    def store_nodes(tx):
        tx.run(
            """
            MERGE (p:Property {url: $url})
            ON CREATE SET p.links_price = $links_price, p.description_price = $description_price, p.links_address = $links_address, p.bautyp = $bautyp, p.boeden = $boeden, p.energy_certificate = $energy_certificate, p.heizung = $heizung, p.links_type = $links_type, p.objekttyp = $objekttyp, p.stockwerke = $stockwerke, p.verfuegbar = $verfuegbar, p.wohnflaeche = $wohnflaeche, p.zimmer = $zimmer, p.zustand = $zustand
            """,
            elem,
        )
        tx.run(
            """
            MERGE (c:Company {company_reference_id: $company_reference_id})
            ON CREATE SET c.company_url = $company_url, c.company_address = $company_address, c.company_name = $company_name
            """,
            elem,
        )
        tx.run(
            """
            MERGE (b:Broker {company_broker_name: $company_broker_name})
            ON CREATE SET b.company_broker_name = $company_broker_name
            """,
            elem,
        )

    session.execute_write(store_nodes)
    print(get_property(session, elem["url"]))
    print(get_company(session, elem["company_reference_id"]))
    print(get_broker(session, elem["company_broker_name"]))

    def store_edges(tx):
        # broker -- employed_by --> company
        tx.run(
            """
            MATCH (b:Broker {company_broker_name: $company_broker_name})
            MATCH (c:Company {company_url: $company_url})
            MERGE (b)-[:employed_by]->(c)
            """,
            elem,
        )
        # broker -- manages (using agreement) --> property
        tx.run(
            """
            MATCH (b:Broker {company_broker_name: $company_broker_name})
            MATCH (p:Property {url: $url})
            MERGE (b)-[r:manages]->(p)
            ON CREATE SET r.last_update = $last_update, r.maklerprovision = CASE WHEN $maklerprovision IS NOT NULL THEN $maklerprovision ELSE 'N/A' END
            """,
            elem,
        )

    session.execute_write(store_edges)
    print(get_employed_by(session, elem["company_broker_name"], elem["company_url"]))
    print(get_manages(session, elem["company_broker_name"], elem["url"]))


session.close()
driver.close()
