#!/bin/bash

# Extract the current version from the local setup.py
echo "Extracting current version"
CURRENT_VERSION=$(grep -oP "(?<=version=\")[^\"]+" setup.py || echo "unknown")
echo "Current Version: $CURRENT_VERSION"

# Find the last commit that modified setup.py and extract the version from that commit
echo "Finding last commit that modified setup.py"
LAST_MODIFIED_COMMIT=$(git log -n 1 --format=format:%H -- setup.py)
PREVIOUS_VERSION=$(git show "$LAST_MODIFIED_COMMIT:setup.py" | grep -oP "(?<=version=\")[^\"]+" || echo "unknown")
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
