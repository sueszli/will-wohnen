import csv
import glob
import shutil
from glob import glob
from pathlib import Path

from graphdatascience import GraphDataScience
from neo4j import GraphDatabase
from tqdm import tqdm

from graphdb_inference import *
from utils import *

"""
initialization
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


@timeit
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
        # company
        tx.execute_write(
            lambda tx, elem: tx.run(
                """
                MERGE (c:Company {company_id: $company_id})
                ON CREATE SET c.company_address = $company_address, c.company_name = $company_name, c.company_url = $company_url
                """,
                elem,
            ),
            elem,
        )
        # broker
        tx.execute_write(
            lambda tx, elem: tx.run(
                """
                MERGE (b:Broker {broker_id: $broker_id})
                """,
                elem,
            ),
            elem,
        )
        # property
        tx.execute_write(
            lambda tx, elem: tx.run(
                """
                MERGE (p:Property {property_id: $property_id})
                ON CREATE SET p.property_availabilty = $property_availabilty, p.property_balcony = $property_balcony, p.property_building_type = $property_building_type, p.property_completion = $property_completion, p.property_condition = $property_condition, p.property_district = $property_district, p.property_energy_certificate = $property_energy_certificate, p.property_features = $property_features, p.property_floor = $property_floor, p.property_flooring = $property_flooring, p.property_garden = $property_garden, p.property_heating = $property_heating, p.property_living_area = $property_living_area, p.property_loggia = $property_loggia, p.property_monthly_costs = $property_monthly_costs, p.property_other_costs = $property_other_costs, p.property_price = $property_price, p.property_rooms = $property_rooms, p.property_status = $property_status, p.property_terrace = $property_terrace, p.property_top_number = $property_top_number, p.property_total_area = $property_total_area, p.property_type = $property_type, p.property_units = $property_units, p.property_usable_area = $property_usable_area, p.property_utilities = $property_utilities
                """,
                elem,
            ),
            elem,
        )
        # company -- employs --> broker
        tx.execute_write(
            lambda tx, elem: tx.run(
                """
                MATCH (c:Company {company_id: $company_id})
                MATCH (b:Broker {broker_id: $broker_id})
                MERGE (c)-[r:employs]->(b)
                """,
                elem,
            ),
            elem,
        )
        # broker -- manages (using agreement) --> property
        tx.execute_write(
            lambda tx, elem: tx.run(
                """
                MATCH (b:Broker {broker_id: $broker_id})
                MATCH (p:Property {property_id: $property_id})
                MERGE (b)-[r:manages]->(p)
                ON CREATE SET r += {agreement_commission_fee: CASE WHEN $agreement_commission_fee IS NOT NULL THEN $agreement_commission_fee END,
                                    agreement_last_updated: CASE WHEN $agreement_last_updated IS NOT NULL THEN $agreement_last_updated END}
                """,
                elem,
            ),
            elem,
        )


"""
graph data science
"""


"""
main loop
"""

reset = True
uri = "bolt://main:7687"
gds = GraphDataScience(uri)

with GraphDatabase.driver(uri).session() as tx:
    if reset:
        tx.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
        print("reset database")
        init_db(tx)
        print("initialized database")

    """
    graph data science
    """


    """
    inference
    """

    outpath_base = Path.cwd() / "data" / "inference"
    shutil.rmtree(outpath_base, ignore_errors=True)
    outpath_base.mkdir(parents=True, exist_ok=True)

    # filename = outpath_base / "district_price_feature_influence.csv"
    # res = tx.execute_read(get_district_price_feature_influence)
    # dump_to_csv(filename, res)

    # filename = outpath_base / "company_city_market_share.csv"
    # res = tx.execute_read(get_company_city_market_share)
    # dump_to_csv(filename, res)

    # res = tx.execute_read(get_company_district_share)
    # filename = outpath_base / "company_district_share.csv"
    # dump_to_csv(filename, res)

    # res = tx.execute_read(get_company_net_worth)
    # filename = outpath_base / "company_net_worth.csv"
    # dump_to_csv(filename, res)

    # res = tx.execute_read(get_broker_city_market_share)
    # filename = outpath_base / "broker_city_market_share.csv"
    # dump_to_csv(filename, res)

    # res = tx.execute_read(get_broker_company_share)
    # filename = outpath_base / "broker_company_share.csv"
    # dump_to_csv(filename, res)

    # res = tx.execute_read(get_broker_property_net_worth)
    # filename = outpath_base / "broker_property_net_worth.csv"
    # dump_to_csv(filename, res)

    # res = tx.execute_read(get_broker_performance_ranking)
    # filename = outpath_base / "broker_performance_ranking.csv"
    # dump_to_csv(filename, res)

    # res = tx.execute_read(get_company_broker_utilization)
    # filename = outpath_base / "company_broker_utilization.csv"
    # dump_to_csv(filename, res)
