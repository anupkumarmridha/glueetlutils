#!/bin/bash

# Fetch the latest changes from the main branch
echo "Fetching origin/main"
git fetch origin +refs/heads/main:refs/remotes/origin/main --depth=1
git reset --hard origin/main

# Extract the current version from setup.py
echo "Extracting current version"
CURRENT_VERSION=$(grep -oP "(?<=version=\")[^\"]+" setup.py || echo "unknown")
echo "Current Version: $CURRENT_VERSION"

# Extract the previous version from the main branch
echo "Extracting previous version"
PREVIOUS_VERSION=$(git show origin/main:setup.py | grep -oP "(?<=version=\")[^\"]+" || echo "unknown")
echo "Previous Version: $PREVIOUS_VERSION"

# Check if versions were successfully detected
if [ "$CURRENT_VERSION" = "unknown" ] || [ "$PREVIOUS_VERSION" = "unknown" ]; then
  echo "Error: Unable to detect version from setup.py"
  exit 1
fi

# Compare the current and previous versions
if [ "$CURRENT_VERSION" = "$PREVIOUS_VERSION" ]; then
  echo "No version change detected. Skipping build and upload."
  echo "version_changed=false" >> $GITHUB_ENV
else
  echo "Version change detected."
  echo "version_changed=true" >> $GITHUB_ENV
fi
