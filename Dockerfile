# Dockerfile
FROM python:3.10-slim

# Install dbt with Snowflake adapter
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

# Set the working directory
WORKDIR /dbt

# Copy the requirements file
COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the dbt project files
COPY . .