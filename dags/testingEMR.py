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
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor, EmrJobFlowSensor

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
        'Name': 'Processa dados do Gov - Cnaes',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode', 'cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=9g',
                     '--conf', 'spark.executor.memoryOverhead=2g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=9g',
                     '--conf', 'spark.driver.memoryOverhead=2g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Cnaes.py'
                     ]
        } #'
    },
    # Motivos
    {
        'Name': 'Processa dados do Gov - Motivos',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode', 'cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=9g',
                     '--conf', 'spark.executor.memoryOverhead=2g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=9g',
                     '--conf', 'spark.driver.memoryOverhead=2g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Motivos.py'
                     ]
        }
    },
    # Municipios
    {
        'Name': 'Processa dados do Gov - Municipios',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode', 'cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=9g',
                     '--conf', 'spark.executor.memoryOverhead=2g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=9g',
                     '--conf', 'spark.driver.memoryOverhead=2g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Municipios.py'
                     ]
        }
    },
    # Naturezas
    {
        'Name': 'Processa dados do Gov - Naturezas',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode', 'cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=9g',
                     '--conf', 'spark.executor.memoryOverhead=2g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=9g',
                     '--conf', 'spark.driver.memoryOverhead=2g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Naturezas.py'
                     ]
        }
    },
    # Paises
    {
        'Name': 'Processa dados do Gov - Paises',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode', 'cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=9g',
                     '--conf', 'spark.executor.memoryOverhead=2g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=9g',
                     '--conf', 'spark.driver.memoryOverhead=2g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Paises.py'
                     ]
        }
    },
    # Qualificacoes
    {
        'Name': 'Processa dados do Gov - Qualificacoes',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode', 'cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=9g',
                     '--conf', 'spark.executor.memoryOverhead=2g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=9g',
                     '--conf', 'spark.driver.memoryOverhead=2g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Qualificacoes.py'
                     ]
        }
    },
    # Empresas
    {
        'Name': 'Processa dados do Gov - Empresas',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode', 'cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=9g',
                     '--conf', 'spark.executor.memoryOverhead=2g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=9g',
                     '--conf', 'spark.driver.memoryOverhead=2g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Empresas.py'
                     ]
        }
    },
    # Estabelecimentos
    {
        'Name': 'Processa dados do Gov - Estabelecimentos',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode', 'cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=9g',
                     '--conf', 'spark.executor.memoryOverhead=2g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=9g',
                     '--conf', 'spark.driver.memoryOverhead=2g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Estabelecimentos.py'
                     ]
        }
    },
    # Simples
    {
        'Name': 'Processa dados do Gov - Simples',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode', 'cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=9g',
                     '--conf', 'spark.executor.memoryOverhead=2g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=9g',
                     '--conf', 'spark.driver.memoryOverhead=2g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Simples.py'
                     ]
        }
    },
    # Socios
    {
        'Name': 'Processa dados do Gov - Socios',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode', 'cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=9g',
                     '--conf', 'spark.executor.memoryOverhead=2g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=9g',
                     '--conf', 'spark.driver.memoryOverhead=2g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Socios.py'
                     ]
        }
    }
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
    

default_args = {
                    "owner": "EMR-Airflow",
                    "depends_on_past": False,
                    "start_date": datetime(2022, 11, 30),
                    "email": ["airflow@airflow.com"],
                    "email_on_failure": False,
                    "email_on_retry": False
                }

# DAG
with DAG('create_emr_cluster',
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
                job_flow_overrides = {
                    "Name": f"{TYPE}-{EVENT}-model-{ENVIRONMENT}-{MODE}",
                    "LogUri": f"s3://256240406578-datalake-{ENVIRONMENT}-raw/emr-logs",
                    "ReleaseLabel": "emr-6.9.0",
                    "Applications": [
                        {
                            "Name": "Spark"
                        },
                        {
                            "Name": "Hadoop"
                        }
                    ],
                    "Instances": {
                        "InstanceGroups": [
                            {
                                "Name": "Master nodes",
                                "Market": "ON_DEMAND",
                                "InstanceRole": "MASTER",
                                "InstanceType": "c5.xlarge",
                                "InstanceCount": 1
                            },
                            {
                                "Name": "Worker nodes",
                                "Market": "ON_DEMAND",
                                "InstanceRole": "CORE",
                                "InstanceType": "r5a.xlarge",
                                "InstanceCount": 3
                            }
                        ],
                        "KeepJobFlowAliveWhenNoSteps": True,
                        "TerminationProtected": False,
                        "Ec2SubnetId": "subnet-00709a3ade46a24c7"
                    },
                    "JobFlowRole": "EMR_EC2_DefaultRole",
                    "ServiceRole": "EMR_DefaultRole",
                    "VisibleToAllUsers": True,
                    "Tags": [
                        {
                            "Key": "BusinessDepartment",
                            "Value": "Pottencial"
                        },
                        {
                            "Key": "CostCenter",
                            "Value": "N/A"
                        },
                        {
                            "Key": "Environment",
                            "Value": f"{ENVIRONMENT}"
                        },
                        {
                            "Key": "ProjectName",
                            "Value": "Data Lake"
                        },
                        {
                            "Key": "TechnicalTeam",
                            "Value": "Arquitetura"
                        }
                    ],
                    "AutoTerminationPolicy": {
                        "IdleTimeout": 120
                    },
                    "StepConcurrencyLevel": 3
                },
                aws_conn_id = "aws_default"
            )


    # Wait running Emr Cluster
    wait_for_emr = EmrJobFlowSensor(
        task_id='wait_for_emr',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states=['WAITING'],
        aws_conn_id='aws_default'
    )


    submit_spark_step = EmrAddStepsOperator(
        task_id='add_spark_step',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
        aws_conn_id='aws_default',
        steps=SPARK_STEPS
    )


    # Wait executions of all steps
    wait_for_steps_completion = EmrStepSensor(
        task_id='wait_for_steps_completion',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
        step_ids=None, # aguarda todos os passos
        aws_conn_id='aws_default'
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
    create_emr_cluster >> wait_for_emr 
    wait_for_emr >> submit_spark_step 
    submit_spark_step >> wait_for_steps_completion
    wait_for_steps_completion >> terminate_emr_cluster
    terminate_emr_cluster >> end_dag