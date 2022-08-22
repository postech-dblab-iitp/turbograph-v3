#!/bin/bash

basedir=$1

vertex=(Organisation.csv Place.csv Tag.csv TagClass.csv)
edge=(Organisation_isLocatedIn_Place.csv Place_isPartOf_Place.csv Tag_hasType_TagClass.csv TagClass_isSubclassOf_TagClass.csv)

for i in "${vertex[@]}"; do
	file_header=`(head -n 1 "${basedir}/$i.original")`
	echo $file_header >> ${basedir}/$i
	tail -n+2 "${basedir}/$i.original" | sort --field-separator='|' --key=1 -n >> ${basedir}/$i
done
for i in "${edge[@]}"; do
	file_header=`(head -n 1 "${basedir}/$i.original")`
	echo $file_header >> ${basedir}/$i
	tail -n+2 "${basedir}/$i.original" | sort --field-separator='|' --key=1,2 -n >> ${basedir}/$i
done
