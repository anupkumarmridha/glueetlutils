#!/bin/bash

# Define variables
DOMAIN_NAME="presidio-data"
ACCOUNT_ID="894811220469"
REGION="us-east-1"
REPO_NAME="datatransformutils-repo"
OUTPUT_FILE="pip_codeartifact_url.txt"

# Fetch the CodeArtifact authorization token
AUTH_TOKEN=$(aws codeartifact get-authorization-token \
  --domain "$DOMAIN_NAME" \
  --domain-owner "$ACCOUNT_ID" \
  --query authorizationToken \
  --output text \
  --region "$REGION")

# Validate the token retrieval
if [ -z "$AUTH_TOKEN" ]; then
  echo "Failed to retrieve the authorization token. Check your AWS credentials and permissions."
  exit 1
fi

# Construct the PyPI repository URL with the proper prefix
PIP_PREFIX="--no-cache-dir --verbose --index-url"
FULL_URL="${PIP_PREFIX} https://aws:${AUTH_TOKEN}@${DOMAIN_NAME}-${ACCOUNT_ID}.d.codeartifact.${REGION}.amazonaws.com/pypi/${REPO_NAME}/simple/"

# Save the full pip-compatible command to a file
echo "$FULL_URL" > "$OUTPUT_FILE"

# Display a success message
echo "The pip command has been generated and saved to $OUTPUT_FILE."
echo "Use the command below for installing packages:"
# cat "$OUTPUT_FILE"
