#!/bin/bash

set -e

echo "Saving files in current working directory: $(pwd)"

docker run -v $(pwd)/artifacts:/usr/src/app/categorize_transactions/artifacts demobelvo
