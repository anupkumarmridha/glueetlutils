#!/bin/bash

# Extract the current version from setup.py using regex
CURRENT_VERSION=$(grep -oP "(?<=version=\")[^\"]+" setup.py || echo "unknown")

# Compare with the previous version in the main branch
git fetch origin main --depth=1
PREVIOUS_VERSION=$(git show origin/main:setup.py | grep -oP "(?<=version=\")[^\"]+" || echo "unknown")

echo "Current Version: $CURRENT_VERSION"
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
