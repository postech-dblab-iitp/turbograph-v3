import requests
import numpy as np
from scipy.sparse import coo_matrix
import torch
import requests

S62_API_URL = "http://localhost:6543/execute-s62"

def test_execute_s62():
    query = """
    MATCH (item:LINEITEM)
    WHERE item.L_SHIPDATE <= date('1998-08-25')
    RETURN
        item.L_RETURNFLAG AS ret_flag,
        item.L_LINESTATUS AS line_stat,
        sum(item.L_QUANTITY) AS sum_qty,
        sum(item.L_EXTENDEDPRICE) AS sum_base_price,
        sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS sum_disc_price,
        sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * (1 + item.L_TAX)) AS sum_charge,
        avg(item.L_QUANTITY) AS avg_qty,
        avg(item.L_EXTENDEDPRICE) AS avg_price,
        avg(item.L_DISCOUNT) AS avg_disc,
        COUNT(*) AS count_order
    ORDER BY
        ret_flag,
        line_stat;
    """
    payload = {"query": query}

    try:
        response = requests.post(S62_API_URL, json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors

        data = response.json()

        print("Query Execution Results:")
        print(f"Elapsed Time: {data.get('elapsed_time', 'N/A')} ms")
        print(f"Result Set Size: {data.get('result_set_size', 0)} rows")
        
        print(data)

    except requests.exceptions.RequestException as e:
        print(f"HTTP Request failed: {e}")
    except ValueError as ve:
        print(f"Response parsing failed: {ve}")

def generate_graph_data(cypher_query, feature_query):
    # Fetch edges
    edge_response = requests.post(S62_API_URL, json={"query": cypher_query})
    edge_response.raise_for_status()
    
    edges = edge_response.json()["results"]

    # Extract src and dst from edges
    src = np.array([edge["src"] for edge in edges])
    dst = np.array([edge["dst"] for edge in edges])
    values = np.ones_like(src)  # All weights set to 1

    # Create COO matrix
    coo = coo_matrix((values, (src, dst)))
    
    # Print some statistics
    print(f"Number of nodes: {coo.shape[0]}")
    print(f"Number of edges: {coo.nnz}")

    # Fetch node features
    feature_response = requests.post(S62_API_URL, json={"query": feature_query})
    feature_response.raise_for_status()
    features = feature_response.json()["results"]

    # Create feature matrix
    feature_matrix = torch.tensor([list(node.values())[1:] for node in features])  # Exclude 'id'

    return coo, feature_matrix

# Generate CUSTOMER-CUSTOMER graph
customer_coo, customer_features = generate_graph_data(
    cypher_query="""
    MATCH (c1:CUSTOMER)-[:PURCHASE_SAME_ITEM]->(c2:CUSTOMER)
    WHERE c1.C_CUSTKEY <> c2.C_CUSTKEY
    RETURN c1.C_CUSTKEY AS src, c2.C_CUSTKEY AS dst
    """,
    feature_query="""
    MATCH (cust:CUSTOMER)
    RETURN
        cust.C_CUSTKEY AS id,
        cust.C_NAME AS name,
        cust.C_ADDRESS AS address,
        cust.C_NATIONKEY AS nationkey,
        cust.C_PHONE AS phone,
        cust.C_ACCTBAL AS acctbal,
        cust.C_MKTSEGMENT AS market_segment,
        cust.C_COMMENT AS comment
    """
)

# Generate ITEM-ITEM graph
item_coo, item_features = generate_graph_data(
    cypher_query="""
    MATCH (item1:PART)-[:BELONG_TO_SAME_ORDER]->(item2:PART)
    WHERE item1.P_PARTKEY <> item2.P_PARTKEY
    RETURN item1.P_PARTKEY AS src, item2.P_PARTKEY AS dst
    """,
    feature_query="""
    MATCH (item:PART)
    RETURN
        item.P_PARTKEY AS id,
        item.P_NAME AS name,
        item.P_MFGR AS manufacturer,
        item.P_BRAND AS brand,
        item.P_TYPE AS type,
        item.P_SIZE AS size,
        item.P_CONTAINER AS container,
        item.P_RETAILPRICE AS retail_price,
        item.P_COMMENT AS comment
    """
)