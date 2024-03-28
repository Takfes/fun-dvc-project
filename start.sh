#!/bin/bash

# Activate the virtual environment
source ./venv/bin/activate

# Initialize Git repository if not already initialized
if [ ! -d .git ]; then
    echo "Initializing a new Git repository..."
    git init
fi

# Check if pre-commit is installed
if ! command -v pre-commit &> /dev/null
then
    echo "pre-commit is not installed. Installing..."
    pip install pre-commit
fi

# Install the pre-commit hooks
echo "Installing pre-commit hooks..."
pre-commit install
