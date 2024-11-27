#!/bin/bash

# Upload the built package to AWS CodeArtifact
twine upload --repository-url "$TWINE_REPOSITORY_URL" dist/*
