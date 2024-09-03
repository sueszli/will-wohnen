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
        # broker -- employed_by --> company
        tx.execute_write(
            lambda tx, elem: tx.run(
                """
                MATCH (b:Broker {broker_id: $broker_id})
                MATCH (c:Company {company_id: $company_id})
                MERGE (b)-[:employed_by]->(c)
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


# def get_district_shares(tx):
#     result = tx.run("""
#     MATCH (p:Property)
#     WHERE p.property_district IS NOT NULL
#     WITH DISTINCT p.property_district AS district
#     WITH collect(district) AS districts

#     UNWIND districts AS district
#     MATCH (c:Company)-[:employed_by]-(b:Broker)-[:manages]->(p:Property {property_district: district})
#     WITH district, count(DISTINCT c) AS company_count

#     WITH collect({district: district, count: company_count}) AS district_counts,
#          sum(company_count) AS total_companies

#     UNWIND district_counts AS dc
#     RETURN dc.district AS district,
#            (toFloat(dc.count) / total_companies * 100) AS company_share_percentage
#     ORDER BY company_share_percentage DESC
#     """)
    
#     district_shares = [{"district": record["district"], "company_share_percentage": record["company_share_percentage"]} for record in result]
#     district_shares.sort(key=lambda x: x["company_share_percentage"], reverse=True)
#     return district_shares

def get_district_shares(tx):
    result = tx.run("MATCH (p:Property) RETURN DISTINCT p.property_district AS district")
    districts = [record["district"] for record in result]

    district_shares = [] # {district: <district>, [company: <company>, percentage: <percentage>]}
    for district in districts:
        result = tx.run(
            """
            MATCH (c:Company)-[:employed_by]-(:Broker)-[:manages]->(p:Property {property_district: $district})
            WITH c.company_name AS company, COUNT(p) AS company_properties
            WITH company, company_properties, 
                toFloat(company_properties) / toFloat(CASE WHEN $total_properties > 0 THEN $total_properties ELSE 1 END) * 100 AS percentage
            ORDER BY percentage DESC
            RETURN {company: company, district: $district, percentage: toFloat(ROUND(100 * percentage) / 100)} AS share
            """,
            district=district, total_properties=tx.run(f"MATCH (p:Property {{property_district: '{district}'}}) RETURN COUNT(p) AS count").single()["count"]
        )
        
        district_shares.extend([record["share"] for record in result])

    return district_shares

uri = "bolt://main:7687"
auth = ("neo4j", "password")
with GraphDatabase.driver(uri, auth=auth).session() as tx:
    reset = False
    if reset:
        tx.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))

    res = tx.execute_read(get_district_shares)
    for r in res:
        print(r)
    # print(res)
