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


"""
initialization
"""


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
queries
"""


def get_company_city_market_share(tx):
    # [company: str, share: float]
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
    # [{district: str, shares: [{company: str, share: float}]}]
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
    # [{company: str, net_worth: float}]
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
    # [{broker: str, share: float}]
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
    # [{company: str, shares: [{broker: str, share: float}]}]
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


def get_broker_property_net_worth(tx):
    # [{broker: str, total_value: float}]
    result = tx.run(
        """
        MATCH (b:Broker)-[:manages]->(p:Property)
        WHERE p.property_price IS NOT NULL
        WITH b, SUM(toFloat(p.property_price)) AS total_value
        ORDER BY total_value DESC
        RETURN b.broker_id AS broker, total_value
        """
    )
    return [{"broker": record["broker"], "total_value": record["total_value"]} for record in result]


def get_broker_collaboration_network(tx, max_depth=3):
    # idea: brokers who have collaborated on managing properties, forming a network of professional relationships --> no chains found
    result = tx.run(
        """
        MATCH path = (b1:Broker)-[:manages]->(:Property)<-[:manages]-(b2:Broker)
        WHERE b1 <> b2
        WITH path, length(path) AS path_length
        WHERE path_length <= $max_depth
        RETURN [node IN nodes(path) | 
                CASE 
                    WHEN node:Broker THEN node.broker_id
                    WHEN node:Property THEN node.property_id
                END] AS collaboration_chain
        """,
        max_depth=max_depth,
    )
    return [record["collaboration_chain"] for record in result]


def get_property_chain_ownership(tx, max_depth=3):
    # where a company owns a property that is managed by a broker who manages another property owned by a different company --> no chains found
    result = tx.run(
        """
        MATCH path = (c1:Company)-[:employs]->(:Broker)-[:manages]->(p1:Property)
        -[:manages]-(:Broker)<-[:employs]-(c2:Company)
        WHERE c1 <> c2
        WITH path, length(path) AS path_length
        WHERE path_length <= $max_depth
        RETURN [node IN nodes(path) | 
                CASE 
                    WHEN node:Company THEN node.company_name 
                    WHEN node:Broker THEN node.broker_id
                    WHEN node:Property THEN node.property_id
                END] AS chain
        """,
        max_depth=max_depth,
    )
    return [record["chain"] for record in result]


def get_company_broker_utilization(tx):
    # [company: str, broker_count: int, property_count: int, efficiency_ratio: float]
    # companies with the highest ratio of properties managed per broker employed
    result = tx.run(
        """
        MATCH (c:Company)-[:employs]->(b:Broker)-[:manages]->(p:Property)
        WITH c, COUNT(DISTINCT b) AS broker_count, COUNT(DISTINCT p) AS property_count
        RETURN c.company_name AS company, 
               broker_count, 
               property_count,
               toFloat(property_count) / broker_count AS efficiency_ratio
        ORDER BY efficiency_ratio DESC
        """
    )
    return [{"company": record["company"], "broker_count": record["broker_count"], "property_count": record["property_count"], "efficiency_ratio": record["efficiency_ratio"]} for record in result]


reset = False
uri = "bolt://main:7687"
auth = ("neo4j", "password")
with GraphDatabase.driver(uri, auth=auth).session() as tx:
    if reset:
        tx.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
        print("reset database")
        init_db(tx)
        print("initialized database")

    outpath_base = Path.cwd() / "data" / "inference"
    outpath_base.mkdir(parents=True, exist_ok=True)

    # # > which companies have most properties in vienna?
    # filename = outpath_base / "company_city_market_share.csv"
    # res = tx.execute_read(get_company_city_market_share)
    # filename.unlink(missing_ok=True)
    # with open(filename, "w") as f:
    #     writer = csv.DictWriter(f, fieldnames=res[0].keys())
    #     writer.writeheader()
    #     writer.writerows(res)

    # # > which companies have most properties in each district?
    # res = tx.execute_read(get_company_district_share)
    # filename = outpath_base / "company_district_share.csv"
    # filename.unlink(missing_ok=True)
    # res = list(itertools.chain(*map(lambda elem: list(map(lambda x: {"district": elem["district"], **x}, elem["shares"])), res)))
    # with open(filename, "w") as f:
    #     writer = csv.DictWriter(f, fieldnames=res[0].keys())
    #     writer.writeheader()
    #     writer.writerows(res)

    # # > which companies have most money in properties?
    # res = tx.execute_read(get_company_net_worth)
    # filename = outpath_base / "company_net_worth.csv"
    # filename.unlink(missing_ok=True)
    # with open(filename, "w") as f:
    #     writer = csv.DictWriter(f, fieldnames=res[0].keys())
    #     writer.writeheader()
    #     writer.writerows(res)

    # # > which brokers have most properties in vienna?
    # res = tx.execute_read(get_broker_city_market_share)
    # filename = outpath_base / "broker_city_market_share.csv"
    # filename.unlink(missing_ok=True)
    # with open(filename, "w") as f:
    #     writer = csv.DictWriter(f, fieldnames=res[0].keys())
    #     writer.writeheader()
    #     writer.writerows(res)

    # # > which brokers have most properties per company?
    # res = tx.execute_read(get_broker_company_share)
    # filename = outpath_base / "broker_company_share.csv"
    # filename.unlink(missing_ok=True)
    # res = list(itertools.chain(*map(lambda elem: list(map(lambda x: {"company": elem["company"], **x}, elem["shares"])), res)))
    # with open(filename, "w") as f:
    #     writer = csv.DictWriter(f, fieldnames=res[0].keys())
    #     writer.writeheader()
    #     writer.writerows(res)

    # > which brokers have the most valuable properties?
    # res = tx.execute_read(get_broker_property_net_worth)
    # filename = outpath_base / "broker_property_net_worth.csv"
    # filename.unlink(missing_ok=True)
    # with open(filename, "w") as f:
    #     writer = csv.DictWriter(f, fieldnames=res[0].keys())
    #     writer.writeheader()
    #     writer.writerows(res)

    # > which companies utilize their brokers most efficiently?
    # res = tx.execute_read(get_company_broker_utilization)
    # filename = outpath_base / "company_broker_utilization.csv"
    # filename.unlink(missing_ok=True)
    # with open(filename, "w") as f:
    #     writer = csv.DictWriter(f, fieldnames=res[0].keys())
    #     writer.writeheader()
    #     writer.writerows(res)
