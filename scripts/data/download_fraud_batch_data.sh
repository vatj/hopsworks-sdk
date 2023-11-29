#!/bin/bash

# Get the absolute path of the script's directory
GIT_DIR="$(git rev-parse --show-toplevel)"

# Create the data directory
mkdir -p "$GIT_DIR/examples/data"

# Download the CSV files
curl -o "$GIT_DIR/examples/data/transactions.csv" https://repo.hops.works/master/hopsworks-tutorials/data/card_fraud_data/transactions.csv
curl -o "$GIT_DIR/examples/data/profiles.csv" https://repo.hops.works/master/hopsworks-tutorials/data/card_fraud_data/profiles.csv
curl -o "$GIT_DIR/examples/data/credit_cards.csv" https://repo.hops.works/master/hopsworks-tutorials/data/card_fraud_data/credit_cards.csv
