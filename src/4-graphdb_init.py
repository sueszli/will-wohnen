import csv
import glob
from glob import glob
from pathlib import Path

from neo4j import GraphDatabase
from tqdm import tqdm

"""
utils
"""


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


"""
database initialization
"""


def show_nodes(tx):
    result = tx.run("MATCH (n) RETURN n")
    for record in result:
        print(record)


def store_nodes(tx, elem):
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


def store_edges(tx, elem):
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


def init_db(tx):
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
        tx.execute_write(store_nodes, elem)
        # print(get_property(tx, elem["url"]))
        # print(get_company(tx, elem["company_reference_id"]))
        # print(get_broker(tx, elem["company_broker_name"]))

        tx.execute_write(store_edges, elem)
        # print(get_employed_by(tx, elem["company_broker_name"], elem["company_url"]))
        # print(get_manages(tx, elem["company_broker_name"], elem["url"]))


"""
inference
"""

# {
#     "agreement_commission_fee": "101370.0",
#     "agreement_last_updated": "23.08.2024 06:10",
#     "broker_id": "georg mels-colloredo",
#     "company_address": "tegetthoffstraße 71010 wien, 01. bezirk, innere stadt",
#     "company_id": "19554",
#     "company_name": "3si makler gmbh",
#     "company_url": "https://www.3si.at",
#     "property_availabilty": null,
#     "property_balcony": "16.97",
#     "property_building_type": "altbau",
#     "property_completion": "2024",
#     "property_condition": "erstbezug",
#     "property_district": "1130",
#     "property_energy_certificate": "d",
#     "property_features": "terrasse, balkon, wintergarten, garten",
#     "property_floor": "1",
#     "property_flooring": "parkett",
#     "property_garden": "112.44",
#     "property_heating": "etagenheizung",
#     "property_id": "https://www.willhaben.at/iad/immobilien/d/eigentumswohnung/wien/wien-1130-hietzing/zeitlose-eleganz-...",
#     "property_living_area": "181.44",
#     "property_loggia": null,
#     "property_monthly_costs": "552.4",
#     "property_other_costs": "87.0",
#     "property_price": "3379000.0",
#     "property_rooms": "4.5",
#     "property_status": "in bau/planung",
#     "property_terrace": "10.2",
#     "property_top_number": "2",
#     "property_total_area": null,
#     "property_type": "wohnung",
#     "property_units": "2",
#     "property_usable_area": "341.96",
#     "property_utilities": "1278.8"
# }

uri = "bolt://main:7687"
auth = ("neo4j", "password")
driver = GraphDatabase.driver(uri, auth=auth)
session = driver.session(database="neo4j")

# wipe = input("Reset database? (y/n): ")
# wipe = True if wipe == "y" else False
wipe = False
if wipe:
    session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
    init_db(session)

session.close()
driver.close()
