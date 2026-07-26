FROM apache/spark:3.5.1-python3

USER root
WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY spark_consumer.py .

ENV PYSPARK_PYTHON=python3

# --packages télécharge le connecteur Kafka<->Spark (et ses dépendances)
# via Maven au premier lancement du conteneur.
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "/app/spark_consumer.py"]
