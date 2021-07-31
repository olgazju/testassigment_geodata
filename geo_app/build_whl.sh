#! /bin/bash

# prepare python geo-utils package
GEO_UTILS_WHL="geoutls-1.1-py3-none-any.whl"
GEO_UTILS_WHL_PATH="$PWD/../airflow_docker"

echo "Remove ${GEO_UTILS_WHL_PATH}/${GEO_UTILS_WHL}"
rm -f ${GEO_UTILS_WHL}/${GEO_UTILS_WHL}

echo "current directory: $PWD"
python3 -m pip install --upgrade setuptools wheel

# clear folder before build
PYTHON_PACKAGE_FOLDER="/dist"
if [ -d "$PYTHON_PACKAGE_FOLDER" ]; then rm -Rf $PYTHON_PACKAGE_FOLDER; fi
python3 setup.py sdist bdist_wheel

cp ./dist/${GEO_UTILS_WHL} ${GEO_UTILS_WHL_PATH}

cp requirements.txt ${GEO_UTILS_WHL_PATH}/requirements.txt
