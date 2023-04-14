#!/bin/bash

./tbgpp-execution-engine/bulkload_json \
	--output_dir:"/data/coco_demo/" \
	--json:"--file_path:/source-data/coco/cocodataset/_0.json --nodes:IMAGES images --nodes:CATEGORIES categories --nodes:ANNOTATIONS annotations --nodes:LICENSES licenses" \
	--relationships:ANNOTATION_CATEGORY /source-data/coco/cocodataset/annotation_category.csv \
	--relationships:ANNOTATION_IMAGE /source-data/coco/cocodataset/annotation_image.csv \
	--relationships:IMAGE_LICENSE /source-data/coco/cocodataset/image_license.csv \
	--relationships:ANNOTATION_IMAGE_BACKWARD /source-data/coco/cocodataset/annotation_image.csv.backward \
	--relationships:ANNOTATION_CATEGORY_BACKWARD /source-data/coco/cocodataset/annotation_category.csv.backward \
	--relationships:IMAGE_LICENSE_BACKWARD /source-data/coco/cocodataset/image_license.csv.backward
