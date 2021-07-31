from airflow.operators.python import task
from airflow.models import DAG
from airflow.utils.dates import days_ago

from airflow import AirflowException

from geo.ingest import ingest

import os 

@task
def ingest_stage():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    result = ingest(os.path.join(dir_path, "..", "input", "Geolife Trajectories 1.3"), os.path.join(dir_path, "..", "output"))

    if not result:
         raise AirflowException('Ingest stage failed')

    #return "result"

@task
def show(xs):
    print(xs)

with DAG(
    "geo_pipeline",
    default_args={'owner': 'airflow'},
    start_date=days_ago(2),
    schedule_interval=None
) as dag:
    xs = ingest_stage()
    show(xs)