FROM postgres:13

# Install the HLL extension package
RUN apt-get update && apt-get install -y \
    postgresql-13-hll \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*