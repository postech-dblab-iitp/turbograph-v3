# S62

Fast, scalable, and flexible OLAP graph database, S62.

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
cd docker
docker build . -t s62-image
./run-docker-container.sh <database folder> <source data folder>
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

Also, if you failed to find image while executing run-docker-container.sh, then change the IMAGE_NAME to the appropriate name.

### Building Project

Before build, please run the following command

```
cd s62-common/third_party/velox
./scripts/setup-ubuntu.sh
```

To build in debug mode, you can run the following commands.

```
mkdir build
cd build/
cmake -GNinja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

To build in release mode, you can run the following commands.

```
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
    ./s62-graph-store/store <storage size (e.g., 10GB, 100gb)>

    # Terminal 2 (runs catalog server)
    cd build
    ./s62-graph-store/catalog_test_catalog_server <db_dir>

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