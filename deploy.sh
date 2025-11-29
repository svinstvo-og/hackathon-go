#!/bin/bash

# --- Configuration ---
REGISTRY="registry.k8s.energyhack.cz"
USERNAME="maksym.koval"
# WARNING: Storing passwords in plain text is a security risk. 
# Consider using environment variables or a secrets manager in production.
PASSWORD="KsVpgRQsXea3CSyCDulU" 
PROJECT="nuclear-elephants/galactic-energy-exchange"

# Accepts version as the first argument, defaults to "v1" if not provided
TAG=${1:-"v1"} 

FULL_IMAGE_NAME="$REGISTRY/$PROJECT:$TAG"

# --- Automation ---

echo "--- Starting Deployment for $FULL_IMAGE_NAME ---"

# 1. Docker Login
# We use --password-stdin to pipe the password directly, bypassing the interactive prompt.
# We use 'sudo' here so the credentials are saved for the root user (needed for the push later).
echo "1. Logging in..."
echo "$PASSWORD" | sudo docker login "$REGISTRY" -u "$USERNAME" --password-stdin

# Check if login succeeded
if [ $? -ne 0 ]; then
    echo "Error: Docker login failed."
    exit 1
fi

# 2. Docker Build
echo "2. Building image..."
sudo docker build -t "$FULL_IMAGE_NAME" .

# Check if build succeeded
if [ $? -ne 0 ]; then
    echo "Error: Docker build failed."
    exit 1
fi

# 3. Docker Push
echo "3. Pushing image..."
sudo docker push "$FULL_IMAGE_NAME"

# Check if push succeeded
if [ $? -ne 0 ]; then
    echo "Error: Docker push failed."
    exit 1
fi

echo "--- Success! Deployed $FULL_IMAGE_NAME ---"
