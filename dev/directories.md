# Project directories

The list below explains directories in the project directory

- `.github/` : Github automation
- `bin/` : binary files and shell scripts for executing the project
- `build/` : build configuration files
- `conf/` : system configuration file templates
- `data/` : data files for running toy examples
- `dev/` : development files, files that may developer may find useful
- `docker/` : docker related files
- `docs/` : design documents and user documents
- `examples/` : getting started examples
- `k8s/` : kubernetes files
- `licenses/` : licence files
- `tools/` : useful tools
- `test/` : testcases for system tests
- `tbgpp-*/` : source codes and test cases of each module
	- `src` : source codes (.cxx) for our native implementations (native namespace)
	- `include` : headers (.hxx) for our native implementations (native namespace)
	- `libABC` : heaviliy modified external libraries we depend on
	- `third_party` : directory for barely modified external libraries we depend on
		- `lib...`