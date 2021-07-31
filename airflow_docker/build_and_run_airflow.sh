#! /bin/bash

set -e

DOCKER_IMAGE_NAME="geo-airflow"
GEO_UTILS_WHL="geoutls-1.1-py3-none-any.whl"

SCRIPT_PATH="$PWD" 
echo "current directory: $SCRIPT_PATH"

PACKAGE_PATH="${SCRIPT_PATH}"/../geo_app
echo "geo app path: $PACKAGE_PATH"

echo "Remove ${SCRIPT_PATH}/${GEO_UTILS_WHL}"
rm -f ${SCRIPT_PATH}/${GEO_UTILS_WHL}

cd $PACKAGE_PATH
python3 -m pip install --upgrade setuptools wheel

# clear folder before build
PYTHON_PACKAGE_FOLDER="/dist"
if [ -d "$PYTHON_PACKAGE_FOLDER" ]; then rm -Rf $PYTHON_PACKAGE_FOLDER; fi
python3 setup.py sdist bdist_wheel

cp ./dist/${GEO_UTILS_WHL} ${SCRIPT_PATH}

cp requirements.txt ${SCRIPT_PATH}/requirements.txt


cd $SCRIPT_PATH

docker build . --tag ${DOCKER_IMAGE_NAME}
#--no-cache

INPUT_DATA_FOLDER=$PWD/geo_data_source
OUTPUT_DATA_FOLDER=$PWD/geo_data_output
DAG_DATA_FOLDER=$PWD/dags
PROCESSED_DATA_FOLDER=$PWD/processed_data

docker run -it  -p 8080:8080 --env "_AIRFLOW_DB_UPGRADE=true" \
--env "_AIRFLOW_WWW_USER_CREATE=true" \
--env "_AIRFLOW_WWW_USER_PASSWORD=admin" \
-v "${INPUT_DATA_FOLDER}:/opt/airflow/input" \
-v "${OUTPUT_DATA_FOLDER}:/opt/airflow/output" \
-v "${DAG_DATA_FOLDER}:/opt/airflow/dags" \
-v "${PROCESSED_DATA_FOLDER}:/opt/airflow/processed_data" \
geo-airflow