import shutil
import time

from airflow.decorators import task
from airflow.models import DAG
from airflow.utils.dates import days_ago

from airflow import AirflowException

from geo.ingest import ingest
from geo.length_dist import analyse_length_dist
from geo.time_gross_dist import analyse_time_gross_dist
from geo.time_net_dist import analyse_time_net_dist

import os 

@task
def ingest_stage():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    result = ingest(os.path.join(dir_path, "..", "input", "Geolife Trajectories 1.3"), os.path.join(dir_path, "..", "output"))

    if not result:
         raise AirflowException('Ingest stage failed')

    #return "result"

@task
def length_analysis_stage():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    result = analyse_length_dist(os.path.join(dir_path, "..", "output"))

    if not result:
         raise AirflowException('Length analysis stage failed')


@task
def time_gross_analysis_stage():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    result = analyse_time_gross_dist(os.path.join(dir_path, "..", "output"))

    if not result:
         raise AirflowException('Time gross analysis stage failed')

@task
def time_net_analysis_stage():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    result = analyse_time_net_dist(os.path.join(dir_path, "..", "output"))

    if not result:
         raise AirflowException('Time gross analysis stage failed')

@task
def get_rid_of_new_data_stage():

    dir_path = os.path.dirname(os.path.realpath(__file__))

    source_dir = os.path.join(dir_path, "..", "input", "Geolife Trajectories 1.3")
    target_dir = os.path.join(dir_path, "..", "processed_data", "Data_{}".format(str(time.time())))

    os.makedirs(target_dir, exist_ok=True)
        
    file_names = os.listdir(source_dir)
        
    for file_name in file_names:
        shutil.move(os.path.join(source_dir, file_name), target_dir)

with DAG(
    "geo_pipeline",
    default_args={'owner': 'airflow'},
    start_date=days_ago(2),
    schedule_interval=None
) as dag:

    ingest_task = ingest_stage()
    length_analysis_task = length_analysis_stage()
    time_gross_analysis_task = time_gross_analysis_stage()
    #time_net_analysis_task = time_net_analysis_stage()
    get_rid_of_new_data_task = get_rid_of_new_data_stage()


#ingest_task >> [length_analysis_task, time_gross_analysis_task, time_net_analysis_task] >> get_rid_of_new_data_task
ingest_task >> [length_analysis_task, time_gross_analysis_task] >> get_rid_of_new_data_task