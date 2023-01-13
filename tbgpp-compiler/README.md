# tbgpp-compiler

- GPorca from greenplum-db repository ([tag 6.23](https://github.com/greenplum-db/gpdb/tree/6.23.0/src/backend/gporca))
- Compiler frontend (lexer/parser) code from kuzudb ([commit bd09081](https://github.com/kuzudb/kuzu/tree/bd0908100d3538e7abd18f0e022bdd1a4c64efae))

## Defining Cypher Language (`Cypher.g4`)

When `src/Cypher.g4` is updated, the parser source code should be re-generated from ANTLR.
The headers and sources in the directory `antlr4_cypher` and should be replaced with new ones.

The updated grammar may also affect the definition of the `CypherLexer` and `CypherParser` class as the function signatures do follow the definitions declared in `Cypher.g4`.

### How to re-generate ANTLR library for changed Cypher specification

TODO how to re-gerenate??