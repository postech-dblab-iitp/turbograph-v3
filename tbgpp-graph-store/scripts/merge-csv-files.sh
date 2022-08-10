#!/bin/bash

basedir=$1

for i in `(ls "${basedir}/original_dataset/")`; do
	for j in `(ls "${basedir}/original_dataset/${i}/" | grep -e "part")`; do
		cat ${basedir}/original_dataset/${i}/${j} >> ${basedir}/${i}.csv
	done
done
