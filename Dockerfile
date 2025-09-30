FROM python:3.11-slim

# Install system dependencies (build + postgres client libs + curl/wget/netcat)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    postgresql-client \
    curl \
    wget \
    netcat-traditional \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Ensure PYTHONPATH includes /app
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Copy everything (source code, requirements, scripts)
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Permission to the wait_for_db script
RUN chmod +x scripts/wait_for_db.sh