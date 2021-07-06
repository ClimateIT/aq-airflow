# Description: Retrieves daily WACCM forecasts
# Changelog:
#   - 2021-07-02: Initial Release

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

from custom_operators import HttpDownloadTimestampedFile

from datetime import timedelta
from dateutil import parser

import defaults

dag_name = "waccm_dag"

# default values for remote url and local path
BASE_URL = "https://www.acom.ucar.edu/waccm/DATA"
BASE_FILENAME = "f.e22.beta02.FWSD.f09_f09_mg17.cesm2_2_beta02.forecast.002.cam.h3.%Y-%m-%d-00000.nc"
LOCAL_DIR = "/data/waccm/"

# Load Airflow variables for the DAG
schedule = Variable.get(f"{dag_name}_schedule", "10 5 * * *")
base_url = Variable.get(f"{dag_name}_base_url", BASE_URL)
base_filename = Variable.get(f"{dag_name}_base_filename", BASE_FILENAME)
local_dir = Variable.get(f"{dag_name}_local_path", LOCAL_DIR)
forecast_days = int(Variable.get(f"{dag_name}_forecast_days", 10))
start_date = parser.parse(Variable.get(f"{dag_name}_start_date", "2021-07-03"))

# create the dag with dummy start and end tasks
dag = DAG(
    dag_name,
    description="Retrieves daily global WACCM forecasts",
    schedule_interval=schedule,
    default_args=defaults.default_args,
    start_date=start_date,
    catchup=False)

start_task = DummyOperator(dag=dag, task_id="start_task")
success_task = DummyOperator(dag=dag, task_id="success")

# point to last download task added to the dag
previous_task = None

# dinamically create the number of tasks corresponding for each forecast day
for d in range(0, forecast_days):
    download_task = HttpDownloadTimestampedFile(
        dag=dag,
        task_id=f"download_day{d}_task",
        step=timedelta(days=d),
        url=base_url + '/' + base_filename,
        local_dir=local_dir
    )

    # chain download tasks to run in sequence
    if previous_task is None:
        start_task >> download_task
    else:
        previous_task >> download_task

    previous_task = download_task

# link end task to last download task
previous_task >> success_task
