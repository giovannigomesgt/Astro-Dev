# Import libraries
import json
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# VARIABLES
EVENT = "salesforce"
TYPE = "batch"
MODE = "historical"

ENVIRONMENT = Variable.get("ENVIRONMENT")
#CONF_CLUSTER = Variable.get("path_conf_cluster") if ENVIRONMENT != "prd" else Variable.get("path_conf_cluster_10_w")
BUCKET_NAME_SCRIPTS = f'pottencial-datalake-{ENVIRONMENT}-raw'


# Spark steps
SPARK_STEPS = [
    {
       {

       },
    },
    {    
    },
    
]

# DAG VARIABLES
STEP_TASK_ID = f"add_steps_{EVENT}_model"  
TAGS = ["DataLake", "EMR", f"AWS{ENVIRONMENT.upper()}EMR",EVENT.capitalize()]  

# SETTING SCHEDULER
if ENVIRONMENT == "prd":
    if MODE == "incremental":
        SCHEDULER = "50 * * * *"
    else:
        SCHEDULER = None

# Cluster scripts
# with open(CONF_CLUSTER) as conf_cluster:
#     JOB_FLOW_OVERRIDES_STR = conf_cluster.read()
#     JOB_FLOW_OVERRIDES_STR = JOB_FLOW_OVERRIDES_STR.replace("{type}",TYPE)\
#                                                    .replace("{event}",EVENT)\
#                                                    .replace("{mode}", MODE)\
#                                                    .replace("{environment}",ENVIRONMENT)    
#     JOB_FLOW_OVERRIDES = json.loads(JOB_FLOW_OVERRIDES_STR)
    
# Set default arguments
default_args = {
                    "owner": "EMR-Airflow",
                    "depends_on_past": False,
                    "start_date": datetime(2022, 11, 30),
                    "email": ["airflow@airflow.com"],
                    "email_on_failure": False,
                    "email_on_retry": False
                }

# DAG
with DAG(DAG_ID='teste',
         default_args = default_args,
         schedule_interval = SCHEDULER,
         max_active_runs = 1,
         catchup = False,
         tags = TAGS,
         description = f"Pipeline Spark on EMR for {EVENT.capitalize()} Model"
    ) as dag:

    # Only display - start_dag
    start_dag = DummyOperator(task_id = "start_dag")

    # Create the EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
                task_id = "create_emr_cluster",
                job_flow_overrides = {},
                aws_conn_id = "aws_default"
            )

    # Add the steps to the EMR cluster
    add_steps = EmrAddStepsOperator(
                task_id = STEP_TASK_ID,
                job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
                aws_conn_id = "aws_default",
                steps = SPARK_STEPS
            )
    last_step = len(SPARK_STEPS) - 1

    # Wait executions of all steps
    check_execution_steps = EmrStepSensor(
                task_id = "check_execution_steps",
                job_flow_id = "{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
                step_id = f"{'{{'} task_instance.xcom_pull(task_ids='add_steps_{EVENT}_model', key='return_value')["
                        + str(last_step)
                        + "] }}",
                aws_conn_id = "aws_default",
            )

    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
                task_id = "terminate_emr_cluster",
                job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
                aws_conn_id = "aws_default",
            )

    # Only display - end_dag
    end_dag = DummyOperator(task_id = "end_dag")

    # Data pipeline flow
    start_dag >> create_emr_cluster 
    create_emr_cluster >> add_steps 
    add_steps >> check_execution_steps 
    check_execution_steps >> terminate_emr_cluster 
    terminate_emr_cluster >> end_dag