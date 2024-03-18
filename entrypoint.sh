#!/bin/bash

# Stop on any error
set -e

python3 categorize_transactions/data_pulling.py
python3 categorize_transactions/train.py
python3 categorize_transactions/predict.py
