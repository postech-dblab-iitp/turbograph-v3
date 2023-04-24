# test

## Preparation

```
python3 -m pip install tabulate
python3 -m pip install colorama
```

## HOWTO run benchmark

First run `store` and `catalog server` in background

Then,
```
python run_s62_benchmark.py TODO
```

## List of Test/Benchmarks

**LDBC Dataset**

- `func` : functionality tests (based on LDBC SF1 dataset, other SF would work though)
- `ldbc` : original LDBC interactive queries
- `ldbc-simplified` : simplified LDBC targeted to run on current s62, ultimately need to be replaced with `ldbc`