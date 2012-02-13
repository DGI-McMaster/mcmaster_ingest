#!/bin/bash

mkdir ./logs 

echo "Running poster ingester: ingest to fedora"

python ./scw_ingest.py ./ &> ./logs/scw_ingest_report.txt

echo "Done."
