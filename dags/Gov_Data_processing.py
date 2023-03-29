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
EVENT = "Gov"
TYPE = "batch"
MODE = "processamento"

ENVIRONMENT = Variable.get("ENVIRONMENT")
# CONF_CLUSTER = Variable.get("path_conf_cluster")

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
                     's3://notebooks-256240406578/sparkcode/etlgov/Cnaes.py'
                     ]
        }
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
                     's3://notebooks-256240406578/sparkcode/etlgov/Motivos.py'
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
                     's3://notebooks-256240406578/sparkcode/etlgov/Municipios.py'
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
                     's3://notebooks-256240406578/sparkcode/etlgov/Naturezas.py'
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
                     's3://notebooks-256240406578/sparkcode/etlgov/Paises.py'
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
                     's3://notebooks-256240406578/sparkcode/etlgov/Qualificacoes.py'
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
                     's3://notebooks-256240406578/sparkcode/etlgov/Empresas.py'
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
                     's3://notebooks-256240406578/sparkcode/etlgov/Estabelecimentos.py'
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
                     's3://notebooks-256240406578/sparkcode/etlgov/Simples.py'
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
                     's3://notebooks-256240406578/sparkcode/etlgov/Socios.py'
                     ]
        }
    }
]


# Acessando o valor da variável json_file_path


# DAG VARIABLES
DAG_ID = f"{EVENT}_model_{ENVIRONMENT}_{MODE}"
STEP_TASK_ID = f"add_steps_{EVENT}_{MODE}"
TAGS = ["DataLake", "EMR", EVENT.capitalize()]

# #Cluster scripts
# with open(CONF_CLUSTER) as conf_cluster:
#     JOB_FLOW_OVERRIDES_STR = conf_cluster.read()
#     JOB_FLOW_OVERRIDES_STR = JOB_FLOW_OVERRIDES_STR.replace("{type}", TYPE)\
#                                                    .replace("{event}", EVENT)\
#                                                    .replace("{mode}", MODE)\
#                                                    .replace("{environment}", ENVIRONMENT)
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
with DAG(DAG_ID,
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=TAGS,
         description=f"Pipeline Spark on EMR for {EVENT.capitalize()} Model"
         ) as dag:

    # Only display - start_dag
    start_dag = DummyOperator(task_id="start_dag")

    # Create the EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides={
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
            "Configurations": [
                {
                    "Classification": "spark-defaults",
                    "Properties": {
                        "spark.submit.deployMode": "cluster",
                        "spark.speculation": "false",
                        "spark.sql.adaptive.enabled": "true",
                        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                        "spark.driver.extraJavaOptions":
                        "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError=\"kill -9 %p\"",
                        "spark.storage.level": "MEMORY_AND_DISK_SER",
                        "spark.rdd.compress": "true",
                        "spark.shuffle.compress": "true",
                        "spark.shuffle.spill.compress": "true"
                    }
                },
                {
                    "Classification": "spark",
                    "Properties": {
                        "maximizeResourceAllocation": "true"
                    }
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
        aws_conn_id="aws_default"
    )
    # return_value:	j-2CRY5GGQV1F45

    # Wait running Emr Cluster
    wait_for_emr = EmrJobFlowSensor(
        task_id='wait_for_emr',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        target_states=['WAITING']
    )

    # Add the steps to the EMR cluster
    add_steps = EmrAddStepsOperator(
        task_id=STEP_TASK_ID,
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS
    )
    last_step = len(SPARK_STEPS) - 1

    # Wait executions of all steps
    check_execution_steps = EmrStepSensor(
        task_id="check_execution_steps",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id=f"{'{{'} task_instance.xcom_pull(task_ids='add_steps_{EVENT}_model', key='return_value')["
        + str(last_step)
        + "] }}",
        aws_conn_id="aws_default",
    )

    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_inzstance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
    )

    # Only display - end_dag
    end_dag = DummyOperator(task_id="end_dag")

    # Data pipeline flow
    start_dag >> create_emr_cluster
    create_emr_cluster >> wait_for_emr
    wait_for_emr >> add_steps
    add_steps >> check_execution_steps
    check_execution_steps >> terminate_emr_cluster
    terminate_emr_cluster >> end_dag
