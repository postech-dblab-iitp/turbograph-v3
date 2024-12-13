import ctypes
import sys
from ctypes import POINTER, c_char_p, c_int, c_long, c_uint64, c_size_t, c_bool
import numpy as np
from scipy.sparse import coo_matrix
import torch

# Load the shared library
try:
    s62 = ctypes.CDLL('/turbograph-v3/build-release/tbgpp-api/c-api/libtbgpp-c-api-shared.so')
except OSError as e:
    print(f"Error loading shared library: {e}")
    exit(1)

# Define types and constants
S62_SUCCESS = 0
S62_END_OF_RESULT = 0

# Define required structures based on the `.h` file
class S62Property(ctypes.Structure):
    pass

S62Property._fields_ = [
    ("label_name", c_char_p),
    ("label_type", c_int),  # Enum s62_metadata_type
    ("order", c_int),
    ("property_name", c_char_p),
    ("property_type", c_int),  # Enum s62_type
    ("property_sql_type", c_char_p),
    ("precision", c_int),
    ("scale", c_int),
    ("next", POINTER(S62Property)),
]

class S62PreparedStatement(ctypes.Structure):
    pass

S62PreparedStatement._fields_ = [
    ("query", c_char_p),
    ("plan", c_char_p),
    ("num_properties", c_size_t),
    ("property", POINTER(S62Property)),
    ("__internal_prepared_statement", ctypes.c_void_p),
]

class S62ResultSetWrapper(ctypes.Structure):
    pass

S62ResultSetWrapper._fields_ = [
    ("num_total_rows", c_size_t),
    ("cursor", c_size_t),
    ("result_set", ctypes.c_void_p),  # Pointer to result set
]

class S62HugeInt(ctypes.Structure):
    _fields_ = [
        ("lower", ctypes.c_uint64),
        ("upper", ctypes.c_int64),
    ]

class S62Decimal(ctypes.Structure):
    _fields_ = [
        ("width", ctypes.c_uint8),
        ("scale", ctypes.c_uint8),
        ("value", S62HugeInt),
    ]

# Map s62_type to corresponding API functions
def fetch_column_value(result_wrapper, col_idx, data_type):
    """
    Fetch the value for the given column index using the appropriate API function based on data_type.
    """
    if data_type == 1:  # S62_TYPE_BOOLEAN
        return s62.s62_get_bool(result_wrapper, col_idx)
    elif data_type == 2:  # S62_TYPE_TINYINT
        return s62.s62_get_int8(result_wrapper, col_idx)
    elif data_type == 3:  # S62_TYPE_SMALLINT
        return s62.s62_get_int16(result_wrapper, col_idx)
    elif data_type == 4:  # S62_TYPE_INTEGER
        return s62.s62_get_int32(result_wrapper, col_idx)
    elif data_type == 5:  # S62_TYPE_BIGINT
        return s62.s62_get_int64(result_wrapper, col_idx)
    elif data_type == 6:  # S62_TYPE_UTINYINT
        return s62.s62_get_uint8(result_wrapper, col_idx)
    elif data_type == 7:  # S62_TYPE_USMALLINT
        return s62.s62_get_uint16(result_wrapper, col_idx)
    elif data_type == 8:  # S62_TYPE_UINTEGER
        return s62.s62_get_uint32(result_wrapper, col_idx)
    elif data_type == 9:  # S62_TYPE_UBIGINT
        return s62.s62_get_uint64(result_wrapper, col_idx)
    elif data_type == 10:  # S62_TYPE_FLOAT
        return s62.s62_get_float(result_wrapper, col_idx)
    elif data_type == 11:  # S62_TYPE_DOUBLE
        return s62.s62_get_double(result_wrapper, col_idx)
    elif data_type in (12, 20, 21, 22):  # S62_TYPE_TIMESTAMP variants
        ts = s62.s62_get_timestamp(result_wrapper, col_idx)
        return ts.micros  # Convert to microseconds
    elif data_type == 13:  # S62_TYPE_DATE
        date = s62.s62_get_date(result_wrapper, col_idx)
        return date.days  # Days since 1970-01-01
    elif data_type == 14:  # S62_TYPE_TIME
        time = s62.s62_get_time(result_wrapper, col_idx)
        return time.micros  # Microseconds since 00:00:00
    elif data_type == 17:  # S62_TYPE_VARCHAR
        value = s62.s62_get_varchar(result_wrapper, col_idx)
        return value.decode('utf-8') if value else None
    elif data_type == 19: # S62_TYPE_DECIMAL
        decimal = s62.s62_get_decimal(result_wrapper, col_idx)
        if not decimal:
            return None
        # Compute the decimal value
        raw_value = decimal.value.upper * (2**64) + decimal.value.lower
        scaled_value = raw_value / (10**decimal.scale)
        return scaled_value
    else:
        return None  # Unsupported or unhandled type


# Function prototypes
s62.s62_connect.argtypes = [c_char_p]
s62.s62_connect.restype = c_int

s62.s62_disconnect.restype = None

s62.s62_prepare.argtypes = [c_char_p]
s62.s62_prepare.restype = POINTER(S62PreparedStatement)

s62.s62_execute.argtypes = [POINTER(S62PreparedStatement), POINTER(POINTER(S62ResultSetWrapper))]
s62.s62_execute.restype = c_size_t

s62.s62_fetch_next.argtypes = [POINTER(S62ResultSetWrapper)]
s62.s62_fetch_next.restype = c_int

s62.s62_get_varchar.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_varchar.restype = c_char_p

s62.s62_close_resultset.argtypes = [POINTER(S62ResultSetWrapper)]
s62.s62_close_resultset.restype = c_int

s62.s62_close_prepared_statement.argtypes = [POINTER(S62PreparedStatement)]
s62.s62_close_prepared_statement.restype = c_int

s62.s62_get_bool.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_bool.restype = c_bool

s62.s62_get_int8.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_int8.restype = ctypes.c_int8

s62.s62_get_int16.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_int16.restype = ctypes.c_int16

s62.s62_get_int32.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_int32.restype = ctypes.c_int32

s62.s62_get_int64.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_int64.restype = ctypes.c_int64

s62.s62_get_uint8.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_uint8.restype = ctypes.c_uint8

s62.s62_get_uint16.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_uint16.restype = ctypes.c_uint16

s62.s62_get_uint32.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_uint32.restype = ctypes.c_uint32

s62.s62_get_uint64.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_uint64.restype = ctypes.c_uint64

s62.s62_get_float.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_float.restype = ctypes.c_float

s62.s62_get_double.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_double.restype = ctypes.c_double

s62.s62_get_timestamp.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_timestamp.restype = ctypes.Structure  # Define s62_timestamp structure as needed

s62.s62_get_date.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_date.restype = ctypes.Structure  # Define s62_date structure as needed

s62.s62_get_time.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_time.restype = ctypes.Structure  # Define s62_time structure as needed

s62.s62_get_varchar.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_varchar.restype = c_char_p

s62.s62_get_decimal.argtypes = [POINTER(S62ResultSetWrapper), c_size_t]
s62.s62_get_decimal.restype = S62Decimal

def extract_properties(prep_stmt):
    """
    Extract and cache properties from the prepared statement.
    """
    properties = []
    current_property = prep_stmt.contents.property
    while current_property:
        prop = current_property.contents
        properties.append({
            "order": prop.order,
            "property_name": prop.property_name.decode('utf-8') if prop.property_name else "NULL",
            "property_type": prop.property_type,
        })
        current_property = prop.next
    return properties

def execute_query(query):
    print(f"Executing query:\n{query}")
    prep_stmt = s62.s62_prepare(query.encode('utf-8'))
    if not prep_stmt:
        print("Error: Failed to prepare query.")
        return []

    # Extract and cache properties
    properties = extract_properties(prep_stmt)
    print("Cached Properties:")
    for prop in properties:
        print(f"Property Name: {prop['property_name']}, Type: {prop['property_type']}")

    # Execute query
    result_wrapper = POINTER(S62ResultSetWrapper)()
    rows = s62.s62_execute(prep_stmt, ctypes.byref(result_wrapper))
    if rows < 0:
        print("Error: Query execution failed.")
        return []

    print(f"Query executed. Total rows: {rows}")
    results = []
    results.append([prop['property_name'] for prop in properties])

    # Fetch results
    while s62.s62_fetch_next(result_wrapper) != S62_END_OF_RESULT:
        row = []
        for prop in properties:
            value = fetch_column_value(result_wrapper, prop['order'], prop['property_type'])
            row.append(value)
        results.append(row)

    # Clean up
    s62.s62_close_resultset(result_wrapper)
    s62.s62_close_prepared_statement(prep_stmt)
    return results

# Main logic
if len(sys.argv) < 2:
    print("Usage: python python-test.py <database_path>")
    exit(1)

database_path = sys.argv[1].encode('utf-8')
if s62.s62_connect(database_path) != S62_SUCCESS:
    print("Error: Failed to connect to the database.")
    exit(1)

print("Successfully connected to the database.")

def execute_query_to_list(query):
    """
    Execute a Cypher query and return the results as a list of dictionaries.
    """
    results = execute_query(query)
    return [dict(zip(results[0], row)) for row in results[1:]]  # Convert rows to dictionaries

def generate_graph_data(cypher_query, feature_query):
    """
    Execute queries to generate graph data and features.
    """
    # Fetch edges
    print("Fetching edges...")
    edge_results = execute_query_to_list(cypher_query)
    src = np.array([edge["src"] for edge in edge_results])
    dst = np.array([edge["dst"] for edge in edge_results])
    values = np.ones_like(src)  # All weights set to 1

    # Create COO matrix
    coo = coo_matrix((values, (src, dst)))

    # Fetch features
    print("Fetching features...")
    feature_results = execute_query_to_list(feature_query)
    feature_matrix = torch.tensor(
        [list(node.values())[1:] for node in feature_results],  # Exclude 'id'
        dtype=torch.float32
    )

    return coo, feature_matrix

# Generate CUSTOMER-CUSTOMER graph
customer_coo, customer_features = generate_graph_data(
    cypher_query="""
    MATCH (c1:CUSTOMER)-[:PURCHASE_SAME_ITEM]->(c2:CUSTOMER)
    WHERE c1.C_CUSTKEY <> c2.C_CUSTKEY
    RETURN c1.C_CUSTKEY AS src, c2.C_CUSTKEY AS dst
    LIMIT 100
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
    LIMIT 100
    """
)

# Generate ITEM-ITEM graph
item_coo, item_features = generate_graph_data(
    cypher_query="""
    MATCH (item1:PART)-[:BELONG_TO_SAME_ORDER]->(item2:PART)
    WHERE item1.P_PARTKEY <> item2.P_PARTKEY
    RETURN item1.P_PARTKEY AS src, item2.P_PARTKEY AS dst
    LIMIT 100
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
    LIMIT 100
    """
)

print("CUSTOMER-CUSTOMER Graph:")
print(f"COO Shape: {customer_coo.shape}")
print(f"Feature Matrix Shape: {customer_features.shape}")

print("ITEM-ITEM Graph:")
print(f"COO Shape: {item_coo.shape}")
print(f"Feature Matrix Shape: {item_features.shape}")

# Disconnect from the database
s62.s62_disconnect()
print("Disconnected from the database.")
