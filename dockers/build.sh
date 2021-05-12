#!/bin/bash
# -- Software Stack Version

SPARK_VERSION="3.0.0"
HADOOP_VERSION="2.7"
JUPYTERLAB_VERSION="2.1.5"

# -- Building the Images


docker build --no-cache\
  -f cluster-base.Dockerfile \
  -t cluster-base .

docker build --no-cache\
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -f spark-base.Dockerfile \
  -t spark-base .

docker build --no-cache\
  -f spark-master.Dockerfile \
  -t spark-master .

docker build --no-cache\
  -f spark-worker.Dockerfile \
  -t spark-worker .

docker build --no-cache\
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  -f jupyterlab.Dockerfile \
  -t jupyterlab .

