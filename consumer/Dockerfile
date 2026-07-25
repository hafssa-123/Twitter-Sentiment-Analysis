FROM python:3.11-slim

WORKDIR /app

# Dépendances système nécessaires pour cassandra-driver
RUN apt-get update && apt-get install -y \
    gcc \
    libev-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .
COPY sentiment_local.py .

CMD ["python", "-u", "consumer.py"]
