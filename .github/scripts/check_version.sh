#!/bin/bash

# Extract the current version
CURRENT_VERSION=$(grep -oP '(?<=version=\")[^\"]+' setup.py || echo "unknown")

# Fetch the main branch
git fetch origin main --depth=1 || { echo "Failed to fetch main branch."; exit 1; }

# Extract the previous version
PREVIOUS_VERSION=$(git show origin/main:setup.py 2>/dev/null | grep -oP '(?<=version=\")[^\"]+' || echo "unknown")

# Debugging output
echo "Current Version: $CURRENT_VERSION"
echo "Previous Version: $PREVIOUS_VERSION"

# Check for errors or mismatches
if [[ "$CURRENT_VERSION" == "unknown" || "$PREVIOUS_VERSION" == "unknown" ]]; then
  echo "Error: Unable to detect version from setup.py. Check your setup.py file."
  exit 1
fi

if [[ "$CURRENT_VERSION" == "$PREVIOUS_VERSION" ]]; then
  echo "No version change detected."
  echo "version_changed=false" >> $GITHUB_ENV
else
  echo "Version change detected."
  echo "version_changed=true" >> $GITHUB_ENV
fi
