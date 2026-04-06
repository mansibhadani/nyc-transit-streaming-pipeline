FROM bitnami/spark:3.4
USER root
WORKDIR /app
COPY spark/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY spark/ .
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,org.apache.spark:spark-avro_2.12:3.4.0", \
     "streaming_job.py"]
