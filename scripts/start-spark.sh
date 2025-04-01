#!/bin/bash
. "$SPARK_HOME/bin/load-spark-env.sh"

case "$SPARK_WORKLOAD" in
  master)
    echo "Starting Spark Master..."
    export SPARK_MASTER_HOST=$(hostname)
    cd "$SPARK_HOME/bin" && exec ./spark-class org.apache.spark.deploy.master.Master \
      --ip "$SPARK_MASTER_HOST" --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG
    ;;
  worker)
    echo "Starting Spark Worker..."
    cd "$SPARK_HOME/bin" && exec ./spark-class org.apache.spark.deploy.worker.Worker \
      --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG
    ;;
  history-server)
    echo "Starting Spark History Server..."
    cd "$SPARK_HOME/sbin" && ./start-history-server.sh
    ;;
  *)
    echo "Invalid workload type: $SPARK_WORKLOAD"
    echo "Valid options: master, worker, submit, history-server"
    exit 1
    ;;
esac
