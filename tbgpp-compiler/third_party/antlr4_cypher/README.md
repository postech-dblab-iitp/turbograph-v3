# antlr4_cypher


## Which Cypher.g4 to use?

Two versions
- Legacy - more of Neo4j syntax
- M21 - standardization, but no crud queries

## Setup docker

Refer https://github.com/antlr/antlr4/tree/master/docker

```
git clone https://github.com/antlr/antlr4.git
cd antlr4
git checkout v4.11.0
cd docker

docker build -t antlr/antlr4 --platform linux/amd64 .
```

### Run docker

```
# need to assign your directory
MY_ANTLR_DIRECTORY=/home/jhko/dev/turbograph-v3/tbgpp-compiler/third_party/antlr4_cypher
cd $MY_ANTLR_DIRECTORY

# execute
docker run --rm -u $(id -u ${USER}):$(id -g ${USER}) -v `pwd`:/work antlr/antlr4 -Dlanguage=Cpp Cypher.g4 -o antlr4/
```