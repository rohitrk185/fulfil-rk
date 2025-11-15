#!/bin/bash
# Build script for Render deployment
set -e

echo "Installing dependencies..."
pip install -r requirements.txt

echo "Running database migrations..."
alembic upgrade head

echo "Build completed successfully!"

