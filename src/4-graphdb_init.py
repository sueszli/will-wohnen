import csv
import glob
from glob import glob
from pathlib import Path

from neo4j import GraphDatabase
from tqdm import tqdm

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


def get_company_city_market_share(tx):
    result = tx.run(
        """
        MATCH (c:Company)-[:employs]->(:Broker)-[:manages]->(p:Property)
        WITH c.company_name AS company, COUNT(DISTINCT p) AS property_count
        WITH COLLECT({company: company, count: property_count}) AS company_counts
        WITH company_counts, REDUCE(total = 0, count IN company_counts | total + count.count) AS total_properties
        UNWIND company_counts AS company_data
        RETURN company_data.company AS company, toFloat(company_data.count) / total_properties AS share
        ORDER BY share DESC
        """
    )
    return [{"company": record["company"], "share": record["share"]} for record in result]


def get_company_district_share(tx):
    result = tx.run(
        """
        MATCH (c:Company)-[:employs]->(:Broker)-[:manages]->(p:Property)
        WITH p.property_district AS district, c.company_name AS company, COUNT(DISTINCT p) AS property_count
        WITH district, COLLECT({company: company, count: property_count}) AS company_counts
        WITH district, company_counts, REDUCE(total = 0, count IN company_counts | total + count.count) AS total_properties
        UNWIND company_counts AS company_data
        WITH district, company_data.company AS company, toFloat(company_data.count) / total_properties AS share
        ORDER BY district, share DESC
        WITH district, COLLECT({company: company, share: share}) AS shares
        RETURN {district: district, shares: shares} AS result
        ORDER BY district
        """
    )
    return [record["result"] for record in result]


def get_company_net_worth(tx):
    result = tx.run(
        """
        MATCH (c:Company)-[:employs]->(:Broker)-[:manages]->(p:Property)
        WITH c.company_name AS company, p.property_price AS price
        WHERE price IS NOT NULL AND price <> ''
        WITH company, toFloat(price) AS numeric_price
        WHERE NOT isNaN(numeric_price)
        RETURN company, SUM(numeric_price) AS net_worth
        ORDER BY net_worth DESC
        """
    )

    return [{"company": record["company"], "net_worth": record["net_worth"]} for record in result]


def get_broker_city_market_share(tx):
    result = tx.run(
        """
        MATCH (b:Broker)-[:manages]->(p:Property)
        WITH b.broker_id AS broker, COUNT(DISTINCT p) AS property_count
        WITH COLLECT({broker: broker, count: property_count}) AS broker_counts
        WITH broker_counts, REDUCE(total = 0, count IN broker_counts | total + count.count) AS total_properties
        UNWIND broker_counts AS broker_data
        RETURN broker_data.broker AS broker, toFloat(broker_data.count) / total_properties AS share
        ORDER BY share DESC
        """
    )
    return [{"broker": record["broker"], "share": record["share"]} for record in result]


def get_broker_company_share(tx):
    result = tx.run(
        """
        MATCH (c:Company)-[:employs]->(b:Broker)-[:manages]->(p:Property)
        WITH c.company_name AS company, b.broker_id AS broker, COUNT(DISTINCT p) AS property_count
        WITH company, COLLECT({broker: broker, count: property_count}) AS broker_counts
        WITH company, broker_counts, REDUCE(total = 0, count IN broker_counts | total + count.count) AS total_properties
        UNWIND broker_counts AS broker_data
        WITH company, broker_data.broker AS broker, toFloat(broker_data.count) / total_properties AS share
        ORDER BY company, share DESC
        WITH company, COLLECT({broker: broker, share: share}) AS shares
        RETURN {company: company, shares: shares} AS result
        ORDER BY company
        """
    )
    return [record["result"] for record in result]


reset = False
uri = "bolt://main:7687"
auth = ("neo4j", "password")
with GraphDatabase.driver(uri, auth=auth).session() as tx:
    if reset:
        tx.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
        print("reset database")
        init_db(tx)
        print("initialized database")

    # > which companies have most properties in vienna?
    # res = tx.read_transaction(get_company_city_market_share)
    # k = 10
    # print(f"top {k} companies by city market share")
    # for elem in res[:k]:
    #     print(f"\t{elem}")

    # > which companies have most properties in each district?
    # res = tx.read_transaction(get_company_district_share)
    # k = 10
    # print(f"top {k} companies by district market share")
    # GREEN = "\033[92m"
    # END = "\033[0m"
    # for elem in res:
    #     print(f"\tdistrict: {elem['district']}")
    #     for cp in elem["shares"][:k]:
    #         cname = cp["company"]
    #         share = cp["share"]
    #         if share >= 0.01:
    #             cname = f"{GREEN}{cname}{END}"
    #         print(f"\t\t{cname}: {share}")

    # > which brokers have most properties in vienna?
    # res = tx.read_transaction(get_broker_city_market_share)
    # k = 10
    # print(f"top {k} brokers by city market share")
    # for elem in res[:k]:
    #     print(f"\t{elem}")

    # > which brokers have most properties per company?
    # res = tx.read_transaction(get_broker_company_share)
    # k = 5
    # print(f"top {k} brokers by company")
    # GREEN = "\033[92m"
    # END = "\033[0m"
    # for elem in res:
    #     cname = elem["company"]
    #     print(f"\tcompany: {cname}")
    #     for cp in elem["shares"][:k]:
    #         bname = cp["broker"]
    #         share = cp["share"]
    #         if share >= 0.5:
    #             bname = f"{GREEN}{bname}{END}"
    #         print(f"\t\t{bname}: {share}")

    # > which companies have most money in properties?
    # res = tx.read_transaction(get_company_net_worth)
    # k = 10
    # print(f"top {k} companies by net worth")
    # for elem in res[:k]:
    #     print(f"\t{elem}")
