#!/bin/bash

# Authenticate with AWS CodeArtifact
TWINE_USERNAME=aws
TWINE_PASSWORD=$(aws codeartifact get-authorization-token \
  --domain presidio-data \
  --domain-owner 894811220469 \
  --region us-east-1 \
  --query authorizationToken \
  --output text)

TWINE_REPOSITORY_URL=$(aws codeartifact get-repository-endpoint \
  --domain presidio-data \
  --domain-owner 894811220469 \
  --repository glueetlutils \
  --region us-east-1 \
  --format pypi \
  --query repositoryEndpoint \
  --output text)

echo "::add-mask::$TWINE_PASSWORD"
echo "TWINE_USERNAME=$TWINE_USERNAME" >> $GITHUB_ENV
echo "TWINE_PASSWORD=$TWINE_PASSWORD" >> $GITHUB_ENV
echo "TWINE_REPOSITORY_URL=$TWINE_REPOSITORY_URL" >> $GITHUB_ENV
