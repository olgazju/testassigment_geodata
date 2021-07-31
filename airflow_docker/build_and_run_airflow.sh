#! /bin/bash

set -e

DOCKER_IMAGE_NAME="geo-airflow"

SCRIPT_PATH="$PWD" 
echo "current directory: $SCRIPT_PATH"

PACKAGE_PATH="${SCRIPT_PATH}"/../geo_app
echo "geo app path: $PACKAGE_PATH"

cd $PACKAGE_PATH

./build_whl.sh


cd $SCRIPT_PATH

docker build . --tag ${DOCKER_IMAGE_NAME}

INPUT_DATA_FOLDER=$PWD/geo_data_source
OUTPUT_DATA_FOLDER=$PWD/geo_data_output
DAG_DATA_FOLDER=$PWD/dags

docker run -it  -p 8080:8080 --env "_AIRFLOW_DB_UPGRADE=true" \
--env "_AIRFLOW_WWW_USER_CREATE=true" \
--env "_AIRFLOW_WWW_USER_PASSWORD=admin" \
-v "${INPUT_DATA_FOLDER}:/opt/airflow/input" \
-v "${OUTPUT_DATA_FOLDER}:/opt/airflow/output" \
-v "${DAG_DATA_FOLDER}:/opt/airflow/dags" \
geo-airflow