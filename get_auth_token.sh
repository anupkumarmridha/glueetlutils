#!/bin/bash

export AWS_REGION=us-east-1

aws codeartifact get-authorization-token \
  --domain presidio-data \
  --domain-owner 894811220469 \
  --query authorizationToken \
  --output text > auth_token.txt

