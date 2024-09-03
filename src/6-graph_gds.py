import shutil
from pathlib import Path

import pandas as pd
from graphdatascience import GraphDataScience
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError


def get_company_similarities(gds) -> pd.DataFrame:
    try:
        gds.graph.drop("company-property-graph")
    except ClientError:
        print("graph does not exist")

    # project the graph
    # company -- ... -> manages --> property
    gds.graph.project.cypher(
        "company-property-graph",
        """
        MATCH (c:Company)
        RETURN id(c) AS id, labels(c) AS labels
        UNION
        MATCH (p:Property)
        RETURN id(p) AS id, labels(p) AS labels
        """,
        """
        MATCH (c:Company)-[:employs]->(:Broker)-[:manages]->(p:Property)
        RETURN id(c) AS source, id(p) AS target, 'MANAGES' AS type
        """,
    )
    company_property_graph = gds.graph.get("company-property-graph")
    print(f"projected graph: {company_property_graph.node_count()} nodes, {company_property_graph.relationship_count()} edges")

    # get node similarity
    threshold = 0.1
    result = gds.nodeSimilarity.write(
        company_property_graph,
        writeRelationshipType="SIMILAR",
        writeProperty="score",
        similarityCutoff=threshold,
    )
    node_pairs_count = result.get("nodePairs", 0)
    write_relationship_count = result.get("writeRelationshipCount", 0)
    print(f"node similarity: {node_pairs_count} pairs, {write_relationship_count} relationships written")

    similar_companies = gds.nodeSimilarity.stream(company_property_graph)
    similar_companies_df = similar_companies.sort_values("similarity", ascending=False).reset_index(drop=True)

    # convert node IDs back to company names
    with GraphDatabase.driver(URI).session() as session:

        def get_company_name(node_id):
            result = session.run("MATCH (c:Company) WHERE id(c) = $id RETURN c.company_name", id=node_id)
            return result.single()["c.company_name"]

        similar_companies_df["company1"] = similar_companies_df["node1"].apply(get_company_name)
        similar_companies_df["company2"] = similar_companies_df["node2"].apply(get_company_name)

    # clean up
    gds.graph.drop("company-property-graph")

    return similar_companies_df[["company1", "company2", "similarity"]]


URI = "bolt://main:7687"
gds = GraphDataScience(URI)

outpath_base = Path.cwd() / "data" / "gds"
shutil.rmtree(outpath_base, ignore_errors=True)
outpath_base.mkdir(parents=True, exist_ok=True)

filename = outpath_base / "similar_companies.csv"
get_company_similarities(gds).to_csv(filename, index=False)
