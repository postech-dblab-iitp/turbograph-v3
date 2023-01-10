# Turbograph v3

Fast, scalable, and flexible HTAP graph database.

## Getting Started

### Tutorial

## Documentation

## Building Project

The project consists of several modules using various platforms. We mainly use `cmake` as project builder, and `ninja` for fast incremental compilation.

```
cd build/
cmake -GNinja -DCMAKE_BUILD_TYPE=Debug ..
ninja -j
```

## Development

For developer guidelines, please refer to documents in `dev/` directory. This directory contains all kinds of information related to development of the project.