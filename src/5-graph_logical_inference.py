import shutil
from pathlib import Path

from neo4j import GraphDatabase

from utils import *


def get_company_city_market_share(tx):
    # > which companies have most properties in vienna?
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
    # > which companies have most properties in each district?
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
    # > which companies have most money in properties?
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
    # > which brokers have most properties in vienna?
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
    # > which brokers have most properties per company?
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
    # > which brokers have the most valuable properties?
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


def get_broker_performance_ranking(tx):
    # > which brokers make the most commission and have the most properties?
    # [{broker: str, total_commission: float, properties_managed: int, avg_commission_per_property: float}]
    result = tx.run(
        """
        MATCH (b:Broker)-[m:manages]->(p:Property)
        WHERE p.property_price IS NOT NULL AND m.agreement_commission_fee IS NOT NULL
        WITH b, SUM(toFloat(p.property_price) * toFloat(m.agreement_commission_fee) / 100) AS total_commission,
            COUNT(p) AS properties_managed
        RETURN b.broker_id AS broker, total_commission, properties_managed,
            total_commission / properties_managed AS avg_commission_per_property
        ORDER BY total_commission DESC
        """
    )
    return [{"broker": record["broker"], "total_commission": record["total_commission"], "properties_managed": record["properties_managed"], "avg_commission_per_property": record["avg_commission_per_property"]} for record in result]


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
    # > which companies utilize their brokers most efficiently? -> companies with the highest ratio of properties managed per broker employed
    # [company: str, broker_count: int, property_count: int, efficiency_ratio: float]
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


def get_district_price_feature_influence(tx):
    # > which districts have the highest average property prices and which features are most common in these districts?
    # [{district: str, avg_price: float, top_features: [(feature: str, count: int)]}]
    result = tx.run(
        """
        MATCH (p:Property)
        WHERE p.property_price IS NOT NULL AND p.property_district IS NOT NULL
        WITH p.property_district AS district, AVG(toFloat(p.property_price)) AS avg_price,
            COLLECT(p.property_features) AS all_features
        UNWIND split(reduce(s = '', f IN all_features | s + ', ' + coalesce(f, '')), ', ') AS feature
        WITH district, avg_price, feature
        WHERE feature <> ''
        WITH district, avg_price, feature, COUNT(*) AS feature_count
        ORDER BY avg_price DESC, feature_count DESC
        WITH district, avg_price, COLLECT({feature: feature, count: feature_count})[..5] AS top_features
        RETURN district, avg_price, top_features
        ORDER BY avg_price DESC
        """
    )
    result = [{"district": record["district"], "avg_price": record["avg_price"], "top_features": record["top_features"]} for record in result]
    for record in result:
        record["top_features"] = [(elem["feature"], elem["count"]) for elem in record["top_features"]]
    return result


URI = "bolt://main:7687"
with GraphDatabase.driver(URI).session() as session:
    outpath_base = Path.cwd() / "data" / "inference"
    shutil.rmtree(outpath_base, ignore_errors=True)
    outpath_base.mkdir(parents=True, exist_ok=True)

    filename = outpath_base / "district_price_feature_influence.csv"
    res = session.execute_read(get_district_price_feature_influence)
    dump_to_csv(filename, res)

    filename = outpath_base / "company_city_market_share.csv"
    res = session.execute_read(get_company_city_market_share)
    dump_to_csv(filename, res)

    res = session.execute_read(get_company_district_share)
    filename = outpath_base / "company_district_share.csv"
    dump_to_csv(filename, res)

    res = session.execute_read(get_company_net_worth)
    filename = outpath_base / "company_net_worth.csv"
    dump_to_csv(filename, res)

    res = session.execute_read(get_broker_city_market_share)
    filename = outpath_base / "broker_city_market_share.csv"
    dump_to_csv(filename, res)

    res = session.execute_read(get_broker_company_share)
    filename = outpath_base / "broker_company_share.csv"
    dump_to_csv(filename, res)

    res = session.execute_read(get_broker_property_net_worth)
    filename = outpath_base / "broker_property_net_worth.csv"
    dump_to_csv(filename, res)

    res = session.execute_read(get_broker_performance_ranking)
    filename = outpath_base / "broker_performance_ranking.csv"
    dump_to_csv(filename, res)

    res = session.execute_read(get_company_broker_utilization)
    filename = outpath_base / "company_broker_utilization.csv"
    dump_to_csv(filename, res)
