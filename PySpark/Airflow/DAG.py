# In order to create a DAG, you must define a DAG file that contains all the details pertaining to DAG tasks, and dependencies must be defined
# in a file (Python script). This is a configuration file specifying the DAG’s structure as code. The five steps that must to be taken to run a DAG are
# shown in Figure:

# Importing the dependencies

from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operations.bash_operators import BashOperator

# Defining the default Arguments

args = {
'owner': 'Jay',
'start_date': airflow.utils.dates.days_ago(3),
'end_date': datetime(2023, 02, 05),
'depends_on_past': False,
'email': ['jay.shah@triveniglobalsoft.com'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=5), 
}

# Creating a DAG

# The third step is to create the DAG itself, which consists of the DAG’s name and schedule interval.


dag = DAG(
'Jay_airflow_dag',
default_args=args,
description='A simple DAG',
# Continue to run DAG once per day
schedule_interval=timedelta(days=1))

# Declaring tasks

# The next step is to declare the tasks (actual jobs) to be executed. All the tasks can be declared and made part of the same DAG created in the preceding step.

t1 = BashOperator(task_id = 'print_date', bash_command='date',dag=dag)

t2 = BashOperator(task_id='sleep', depends_on_past=False, bash_command='sleep 5', dag=dag)

# Mentioning Dependencies

# The final step is to set the order of task execution. They can be either parallel or sequential tasks. There are multiple ways in which the tasks can be defined.

t1 >> t2

# Once all the preceding steps have been completed, we can start Airflow and access the web UI.

