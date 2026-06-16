FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY pyproject.toml .
COPY model.yaml .
COPY src ./src

RUN pip install --no-cache-dir -e ".[spark,dev]"

EXPOSE 8000

CMD ["uvicorn", "lemonade_backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
