FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dbt project structure (excludes packages, target, logs via .dockerignore)
COPY transform/dbt_project.yml transform/dbt_project.yml
COPY transform/dependencies.yml transform/dependencies.yml
COPY transform/package-lock.yml transform/package-lock.yml
COPY transform/seeds/ transform/seeds/
COPY transform/models/ transform/models/
# Container-specific profiles.yml (replaces the gitignored local one)
COPY docker/profiles.yml transform/profiles.yml

RUN cd transform && dbt deps

RUN mkdir -p /app/credentials

COPY orchestration/ orchestration/
COPY serve.py .

CMD ["python", "serve.py"]
