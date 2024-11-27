#!/bin/bash

# Debugging start
echo "Fetching origin/main"
git fetch origin main --depth=1

echo "Extracting current version"
CURRENT_VERSION=$(grep -oP "(?<=version=\")[^\"]+" setup.py || echo "unknown")
echo "Current Version: $CURRENT_VERSION"

echo "Extracting previous version"
PREVIOUS_VERSION=$(git show origin/main:setup.py | grep -oP "(?<=version=\")[^\"]+" || echo "unknown")
echo "Previous Version: $PREVIOUS_VERSION"

if [ "$CURRENT_VERSION" = "unknown" ] || [ "$PREVIOUS_VERSION" = "unknown" ]; then
  echo "Error: Unable to detect version from setup.py"
  exit 1
fi

if [ "$CURRENT_VERSION" = "$PREVIOUS_VERSION" ]; then
  echo "No version change detected. Skipping build and upload."
  echo "version_changed=false" >> $GITHUB_ENV
else
  echo "Version change detected."
  echo "version_changed=true" >> $GITHUB_ENV
fi
