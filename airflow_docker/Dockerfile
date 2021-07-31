FROM apache/airflow:2.1.2-python3.8

ARG AIRFLOW_HOME=/opt/airflow
ARG WHL_FILE=geoutls-1.1-py3-none-any.whl

#COPY --chown=airflow:root dags/geo_pipeline_dag.py ${AIRFLOW_HOME}/dags

COPY entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh

COPY ${WHL_FILE} ${AIRFLOW_HOME}/${WHL_FILE}
RUN pip install ${WHL_FILE}

COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install -r requirements.txt 

RUN mkdir -p ${AIRFLOW_HOME}/input
RUN mkdir -p ${AIRFLOW_HOME}/output

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
