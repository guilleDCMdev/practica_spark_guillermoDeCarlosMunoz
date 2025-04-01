# Stage 1: Builder
FROM openjdk:11.0.11-jre-slim-buster as builder

# Install necessary packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl vim wget software-properties-common ssh net-tools ca-certificates python3-setuptools \
    python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy && \
    rm -rf /var/lib/apt/lists/* 

# Ensure python points to python3
RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

# Install PySpark
RUN pip3 install pyspark

# Set environment variables
ENV SPARK_VERSION=3.5.5 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1 \
    SPARK_NO_DAEMONIZE=true

# Download and extract Apache Spark
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    mkdir -p $SPARK_HOME && \
    tar -xf apache-spark.tgz -C $SPARK_HOME --strip-components=1 && \
    rm apache-spark.tgz

# Download additional S3 jar
RUN wget --no-verbose -O $SPARK_HOME/jars/s3-2.31.4.jar "https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.31.4/s3-2.31.4.jar"

# Stage 2: Final image
FROM builder as apache-spark

WORKDIR $SPARK_HOME

# Set Spark service environment variables
ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
    SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
    SPARK_WORKER_WEBUI_PORT=8080 \
    SPARK_WORKER_PORT=7000 \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_WORKLOAD="master"

# Expose necessary ports
EXPOSE 8080 7077 7000

# Create log directory and link logs to stdout for container logging and history-server data
RUN mkdir -p $SPARK_LOG_DIR && \
    mkdir -p /opt/spark/history &&\
    touch $SPARK_MASTER_LOG && \
    touch $SPARK_WORKER_LOG && \
    ln -sf /dev/stdout $SPARK_MASTER_LOG && \
    ln -sf /dev/stdout $SPARK_WORKER_LOG

# Copy and prepare the entrypoint script
COPY ./scripts/start-spark.sh /start-spark.sh
RUN sed -i 's/\r$//g' /start-spark.sh && chmod +x /start-spark.sh

CMD ["/bin/bash", "/start-spark.sh"]
