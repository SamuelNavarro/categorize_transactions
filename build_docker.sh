#!/bin/bash

# Define your image name
IMAGE_NAME="demobelvo"

# Build the Docker image
echo "Building Docker image: $IMAGE_NAME"
docker build -t $IMAGE_NAME -f docker/Dockerfile .

echo "Docker image $IMAGE_NAME built successfully."
