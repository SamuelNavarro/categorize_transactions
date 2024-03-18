# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Update and install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install pip and poetry
RUN pip install --upgrade pip \
    && curl -sSL https://install.python-poetry.org | python3 -

# Ensure Poetry is in PATH, it's installed to $HOME/.local/bin
ENV PATH="${PATH}:/root/.local/bin"

# Copy only necessary files
COPY pyproject.toml poetry.lock ./

# Install project dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev

COPY . $WORKDIR

# Copy the entrypoint script and make it executable
COPY entrypoint.sh /usr/src/app/
RUN chmod +x /usr/src/app/entrypoint.sh

# Set the entrypoint to run the scripts
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
