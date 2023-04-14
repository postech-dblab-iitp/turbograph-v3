#!/bin/bash

SUFFIX=""
SF=1

./tbgpp-execution-engine/bulkload_json \
	--output_dir:"/data/ckg/" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Protein_structure.json --nodes:Protein_structure" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_User.json --nodes:User" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_GWAS_study.json --nodes:GWAS_study" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Pathway.json --nodes:Pathway" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Publication.json --nodes:Publication" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Project.json --nodes:Project" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Metabolite.json --nodes:Metabolite" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Functional_region.json --nodes:Functional_region" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Complex.json --nodes:Complex" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Transcript.json --nodes:Transcript" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Chromosome.json --nodes:Chromosome" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Somatic_mutation.json --nodes:Somatic_mutation" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Known_variant.json --nodes:Known_variant" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Clinically_relevant_variant.json --nodes:Clinically_relevant_variant" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Modified_protein.json --nodes:Modified_protein" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Peptide.json --nodes:Peptide" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Food.json --nodes:Food" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Timepoint.json --nodes:Timepoint" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Amino_acid_sequence.json --nodes:Amino_acid_sequence" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Cellular_component.json --nodes:Cellular_component" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Molecular_function.json --nodes:Molecular_function" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Phenotype.json --nodes:Phenotype" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Gene.json --nodes:Gene" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Modification.json --nodes:Modification" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Protein.json --nodes:Protein" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Experiment.json --nodes:Experiment" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Tissue.json --nodes:Tissue" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Disease.json --nodes:Disease" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Biological_process.json --nodes:Biological_process" \
	--jsonl:"--file_path:/source-data/ckg/ckg_node_Experimental_factor.json --nodes:Experimental_factor"
	#--jsonl:"--file_path:/source-data/ckg/ckg_node_Clinical_variable.json --nodes:Clinical_variable" \
	#--jsonl:"--file_path:/source-data/ckg/ckg_node_Subject.json --nodes:Subject" \
	#--jsonl:"--file_path:/source-data/ckg/ckg_node_Drug.json --nodes:Drug" \
	#--jsonl:"--file_path:/source-data/ckg/ckg_node_Biological_sample.json --nodes:Biological_sample" \
	#--jsonl:"--file_path:/source-data/ckg/ckg_node_Analytical_sample.json --nodes:Analytical_sample" \
