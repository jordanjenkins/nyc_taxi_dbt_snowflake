# Dockerfile
FROM python:3.10-slim

# Install dbt with Snowflake adapter
RUN apt-get update && \
    apt-get install -y git && \
    pip install --upgrade pip && \
    pip install dbt-snowflake && \
    apt-get clean

# Set the working directory
WORKDIR /dbt

# Default command
ENTRYPOINT ["dbt"]