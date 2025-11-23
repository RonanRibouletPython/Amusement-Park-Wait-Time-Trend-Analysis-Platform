# Use a lightweight Python 3.11 image
FROM python:3.11-slim

# Set environment variables
# PYTHONUNBUFFERED=1 ensures logs show up immediately in Cloud Logging
ENV PYTHONUNBUFFERED=1 \
    POETRY_VERSION=1.8.2 \
    POETRY_VIRTUALENVS_CREATE=false

# Install system dependencies (curl to install poetry)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

# Set work directory
WORKDIR /app

# Copy dependency files first (for caching)
COPY pyproject.toml poetry.lock ./

# Install dependencies (no dev dependencies for production)
RUN poetry install --no-root --only main

# Copy the rest of the application code
COPY src/ ./src/
COPY shared/ ./shared/
COPY tools/ ./tools/
# Copy GCP credentials
COPY secrets/sa-key.json /app/sa-key.json
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/sa-key.json"

# Set environment variables for Python to look in the app directory for modules
ENV PYTHONPATH=/app

# Command to run the pipeline
CMD ["python", "src/data_orchestration.py"]