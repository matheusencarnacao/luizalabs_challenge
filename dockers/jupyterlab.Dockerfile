FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.0.0
ARG jupyterlab_version=2.1.5

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    apt-get install -y python3-venv && \
    apt-get install -y zip && \
    pip3 install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# -- Runtime

EXPOSE 8888

ENV OUTPUT_CSV=${SHARED_WORKSPACE}/csv

WORKDIR ${SHARED_WORKSPACE}

ADD setup_and_submit.sh ${SHARED_WORKSPACE}/setup_and_submit.sh

CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=