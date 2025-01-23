# S62

Fast, scalable, and flexible OLAP graph database, S62.

## Notes

We build the system over
- Duckdb: 0.3.4
- Kuzu: 0.3.1

## Getting Started

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

For executing LDBC,

```
cp scripts/run-ldbc.sh build
cd build
bash run-ldbc.sh <db_dir>
```

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