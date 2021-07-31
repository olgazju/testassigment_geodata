FROM apache/airflow:2.1.2-python3.8


USER root

RUN apt-get -y update && \
    apt-get install --no-install-recommends -y openjdk-11-jre-headless ca-certificates-java && \
    rm -rf /var/lib/apt/lists/*

# Setup JAVA_HOME -- useful for docker commandline
#ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
#RUN export JAVA_HOME

USER airflow


ARG AIRFLOW_HOME=/opt/airflow
ARG WHL_FILE=geoutls-1.1-py3-none-any.whl


COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install -r requirements.txt 

COPY ${WHL_FILE} ${AIRFLOW_HOME}/${WHL_FILE}
RUN pip install ${WHL_FILE}

COPY entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh

RUN mkdir -p ${AIRFLOW_HOME}/input
RUN mkdir -p ${AIRFLOW_HOME}/output
RUN mkdir -p ${AIRFLOW_HOME}/processed_data

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
