# tbgpp-compiler

- Parser code from kuzudb.


## Defining Cypher Language (`Cypher.g4`)

When `src/Cypher.g4` is updated, the parser source code should be re-generated from ANTLR.
The headers and sources in the directory `antlr4_cypher` and should be replaced with new ones.

The updated grammar may also affect the definition of the `CypherLexer` and `CypherParser` class as the function signatures do follow the definitions declared in `Cypher.g4`.

### How to re-generate ANTLR library for changed Cypher specification

TODO how to re-gerenate??