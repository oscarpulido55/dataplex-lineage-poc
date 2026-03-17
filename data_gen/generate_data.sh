#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Setting up python venv..."
python3 -m venv venv
source venv/bin/activate
pip install pandas pyarrow

echo "Generating local parquet files..."
python generate_data.py

echo "Parquet files successfully generated in data_gen/!"
echo "Please run 'terraform apply' in the terraform/ directory to provision buckets and upload these files."
