FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    postgresql-client \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Set PYTHONPATH to include /app
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Copy only the requirements file to leverage Docker cache
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Permission to the wait_for_db script
RUN chmod +x scripts/wait_for_db.sh