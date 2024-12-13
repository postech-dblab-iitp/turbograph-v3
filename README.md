# Turbograph v3

Fast, scalable, and flexible OLAP graph database, S62.

## Abstract

The absence of strict requirements to define schemas formally before use in the property graph model offers great flexibility facilitating painless graph data loading and update.
However, such schemalessness comes at a cost during the operations. 
It complicates the design of efficient storage and becomes a major bottleneck during the graph query processing.
To overcome these challenges, we designed and developed a system called S62 to bridge the gap between the flexibility of schemaless data and the efficiency of schema-aware storage and query optimization.
S62 utilizes a novel approach of internally extracting and clustering schemas based on similarity.
This technique, termed homogeneous relation chunking, organizes data into groups stored in a columnar format, enabling efficient vectorized query execution.
Moreover, S62 incorporates the industrial-strength Orca optimizer, minimally modified to be graph-aware, allowing for advanced query optimization techniques developed in the relational database domain.

## Getting Started

### Dataset

Supported File Formats

We currently support the following file formats:
- **CSV**
- **JSON**

CSV Format Requirements
- Files must include headers specifying the names and data types of each column.
- Edge file must have :START_ID(TYPE)|:END_ID(TYPE) columns. In case of backward file, it should have :END_ID(TYPE)|:START_ID(TYPE) columns.

JSON Format Requirements:
- Files must contain a list of objects. Each object should include consistent labels and possess unique properties.

To experiment with a typical dataset, you can download the LDBC SF1 dataset from the following [link](https://drive.google.com/file/d/1PqXw_Fdp9CDVwbUqTQy0ET--mgakGmOA/view?usp=drive_link).

### Docker Setting

We provide a docker image for the project. You can build the image using the following command.

```
git clone https://github.com/postech-dblab-iitp/turbograph-v3.git
cd turbograph-v3
git checkout remotes/origin/main
cd docker
docker build . -t turbograph-image
./run-docker-example.sh <database folder> <source data folder>
```

Directory Definitions

- Database Folder: This directory contains the database files. In a Docker environment, this folder can be accessed at /data.
- Source Data Folder: This directory is designated for storing source CSV files. In a Docker environment, access this folder via /source-data.

#### Error handling

If you failed in bulding docker image, change change

```
# (boost)
RUN apt-get install -y libboost-all-dev=1.71.0.0ubuntu2

to

# (boost)
RUN apt-get update
RUN apt-get update --fix-missing
RUN apt-get install -y libboost-all-dev=1.71.0.0ubuntu2
```

Also, if you failed to find image while executing run-docker-example.sh, then change the IMAGE_NAME to the appropriate name.

### Building Project

After docker setup, please execute this. You only need to execute once.

```
cd /turbograph-v3
cd tbgpp-common/third_party/velox
./scripts/setup-ubuntu.sh
```

To build in debug mode, you can run the following commands.
```
cd /turbograph-v3
mkdir build
cd build/
cmake -GNinja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

To build in release mode, you can run the following commands.

```
cd /turbograph-v3
mkdir build
cd build/
cmake -GNinja -DCMAKE_BUILD_TYPE=Release ..
ninja
```

### Executing S62

After building the project, you can run the following command to execute S62.

Executing is comprised of three steps, loading dataset, executing client, building statistics.

1. Loading Dataset

    You have to use three terminals for this.

    ```
    # Terminal 1 (runs storage server)
    cd build
    ./tbgpp-graph-store/store <storage size (e.g., 10GB, 100gb)>

    # Terminal 2 (runs catalog server)
    cd build
    ./tbgpp-graph-store/catalog_test_catalog_server <db_dir>

    # Terminal 3 (runs bulkloading process)
    cp scripts/bulkload/run-ldbc-bulkload.sh build
    cd build
    bash run-ldbc-bulkload.sh <db_dir> <data_dir>
    ```

    db_dir is a directory where the database will located.

    data_dir is a directory where the source data is located.

2. Executing Client

    You have to run analyze to make optimizers use statistics.

    ```
    cp scripts/runner/run-ldbc.sh build
    cd build
    bash run-ldbc.sh <db_dir>
    ```

    You will see `Turbograph-S62 >> ` prompt. You can execute queries here.

## Execution Options

- `--workspace: <workspace>`: Specifies the workspace directory.
- `--query: <query>`: Specifies the query file.
- `--debug-orca`: Enables debug mode for the Orca optimizer.
- `--explain`: Prints detailed information about the query execution plan.
- `--profile`: Prints the query plan profile output.
- `--dump-output <output path>`: Dumps the query output to the specified path.
- `--num-iterations: <num iterations>`: Specifies the number of iterations for the query.
- `--disable-merge-join`: Disables the merge join operator (default optimizer mode)
- `--join-order-optimizer:<exhaustive, exhaustive2, query, greedy>`: Specifies the join order optimizer mode.

## Execution Commands

- `:exit`: Exits the client.
- `analyze`: Update the statistics
- `flush_file_meta`: increase client initialization speed by flushing file metadata

## Query Support

- COUNT
    - COUNT(): Not Supported
    - COUNT(*): Supported
    - COUNT(column_name): Supported


## API Documentation

### Supported APIs

1. **C/C++ APIs**:
   - The C/C++ APIs follow DuckDB's API design.
   - Refer to the DuckDB API documentation for detailed usage examples.

2. **Socket APIs**:
   - A simple socket-based API for client-server architecture.
   - Socket APIs enable communication between the client and the S62 server.
   - Refer to the `tbgpp-api/server` directory for implementation details.

### Socket API List

The following API identifiers (`API_ID`) are supported for socket communication:

- **PrepareStatement (`API_ID = 0`)**: Prepares a query.
- **ExecuteStatement (`API_ID = 1`)**: Executes a prepared query.
- **Fetch (`API_ID = 2`)**: Fetches a single row from the result set.
- **FetchAll (`API_ID = 3`)**: Fetches all rows from the result set.

### How to Send Requests to the Socket Server

1. **Connect to the Socket**:
   - Use a TCP socket to connect to the S62 server at the configured address and port.
   - Default port: `8080`.

2. **Message Format**:
   - Each request message begins with the `API_ID`, followed by the payload (e.g., query text, client ID).

3. **Example**:
   - **PrepareStatement**: Send the query string.
   - **ExecuteStatement**: Send the prepared client ID.
   - **FetchAll**: Retrieve all rows of the result set in CSV format.

### How to Run Socket Server

1. **Start the Store**:
     ```bash
     cd /turbograph-v3/build
     ./tbgpp-graph-store/store <storage size (e.g., 10GB, 100gb)>
     ```

2. **Start the Socket Server**:
     ```bash
     cd /turbograph-v3/build/tbgpp-api/server
     ./socket_server_run <workspace> # e.g., ./socket_server_run /data/tpch/sf1
     ```

### Python Flask Server/Client Examples

The `tbgpp-api/server/test/python-example` directory contains examples for integrating with the Flask server. This includes:

- **Flask Server**:
  - Handles API requests and communicates with the S62 server over sockets.
  - Converts query results (in CSV format) to JSON for JavaScript compatibility.
- **Flask Client**:
  - Demonstrates how to send queries and interpret the JSON responses.

Note that you SHOULD run socket server before running the Flask server.

### Running the Flask Server

1. **Start the Flask Server**:
   - Run the server script:
     ```bash
     python3 flask-server.py
     ```
   - The server listens on `http://localhost:6543` by default.

2. **Example Endpoint**:
   - `/execute-s62` (POST): Executes an S62 query and returns the results.

3. **Query Format**:
   - JSON payload with the `query` field:
     ```json
     {
       "query": "MATCH (item:LINEITEM) WHERE item.L_SHIPDATE <= date('1998-08-25') RETURN ..."
     }
     ```

### Running the Flask Client

1. **Run the Client**:
   - Use the provided test script to send a query to the server:
     ```bash
     python3 flask-client.py
     ```

2. **Expected Output**:
   - The client sends a query to the Flask server, and the response includes:
     ```json
     {
       "elapsed_time": 960,
       "property_names": ["ret_flag", "line_stat", "sum_qty", "sum_base_price", ...],
       "result_set_size": 4,
       "results": "A|F|18064037556|56586554400.73|53758257134.8700|55909065222.827692|12217.871546|38273.129735|0.049985|1478493\nN|F..."
     }
     ```