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

from airflow.providers.amazon.aws.operators.emr import EMRStepOperator


# VARIABLES
EVENT = "gov"
TYPE = "batch"
MODE = "historical"

ENVIRONMENT = Variable.get("ENVIRONMENT")
# CONF_CLUSTER = Variable.get("path_conf_cluster_gov")
BUCKET_NAME_SCRIPTS = f'pottencial-datalake-{ENVIRONMENT}-raw'


# Spark steps
SPARK_STEPS = [
    # Cnaes
    {
        'Name': 'Processa dados do Gov - Cnaes',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--deploy-mode cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=5g',
                     '--conf', 'spark.executor.memoryOverhead=1g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=5g',
                     '--conf', 'spark.driver.memoryOverhead=1g',
                     '--conf', 'spark.default.parallelism=8',
                     '--conf', 'spark.sql.sources.partitionOverwriteMode=dynamic',
                     '--conf', 'spark.driver.maxResultSize=5g',
                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                     '--conf', 'spark.dynamicAllocation.enabled=false',
                     f's3://256240406578-datalake-{ENVIRONMENT}-raw/sparkcode/etlgov/Cnaes.py'
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
                     '--deploy-mode cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=5g',
                     '--conf', 'spark.executor.memoryOverhead=1g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=5g',
                     '--conf', 'spark.driver.memoryOverhead=1g',
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
                     '--deploy-mode cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=5g',
                     '--conf', 'spark.executor.memoryOverhead=1g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=5g',
                     '--conf', 'spark.driver.memoryOverhead=1g',
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
                     '--deploy-mode cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=5g',
                     '--conf', 'spark.executor.memoryOverhead=1g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=5g',
                     '--conf', 'spark.driver.memoryOverhead=1g',
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
                     '--deploy-mode cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=5g',
                     '--conf', 'spark.executor.memoryOverhead=1g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=5g',
                     '--conf', 'spark.driver.memoryOverhead=1g',
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
                     '--deploy-mode cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=5g',
                     '--conf', 'spark.executor.memoryOverhead=1g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=5g',
                     '--conf', 'spark.driver.memoryOverhead=1g',
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
                     '--deploy-mode cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=5g',
                     '--conf', 'spark.executor.memoryOverhead=1g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=5g',
                     '--conf', 'spark.driver.memoryOverhead=1g',
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
                     '--deploy-mode cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=5g',
                     '--conf', 'spark.executor.memoryOverhead=1g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=5g',
                     '--conf', 'spark.driver.memoryOverhead=1g',
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
                     '--deploy-mode cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=5g',
                     '--conf', 'spark.executor.memoryOverhead=1g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=5g',
                     '--conf', 'spark.driver.memoryOverhead=1g',
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
                     '--deploy-mode cluster',
                     '--conf', 'spark.executor.cores=4',
                     '--conf', 'spark.executor.memory=5g',
                     '--conf', 'spark.executor.memoryOverhead=1g',
                     '--conf', 'spark.executor.instances=1',
                     '--conf', 'spark.driver.cores=4',
                     '--conf', 'spark.driver.memory=5g',
                     '--conf', 'spark.driver.memoryOverhead=1g',
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
TAGS = ["DataLake", "EMR", f"AWS{ENVIRONMENT.upper()}EMR", EVENT.capitalize()]

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
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1,
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
            "Name": "{type}-{event}-model-{environment}-{mode}",
                    "LogUri": "s3://256240406578-datalake-{environment}-raw/emr-logs",
                    "ReleaseLabel": "emr-6.9.0",
            "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}, {"Name": "JupyterHub"}],
                    "Configurations": [
                {
                    "Classification": "spark-env",
                    "Properties": {},
                    "Configurations": [{
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3",
                            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                        }
                    }]
                },
                {
                    "Classification": "spark-hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    }
                },
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
                },
                {
                    "Classification": "emrfs-site",
                    "Properties": {
                        "fs.s3.maxConnections": "1000"
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
                {"Key": "BusinessDepartment", "Value": "Pottencial"}, {
                    "Key": "CostCenter", "Value": "N/A"}, {"Key": "environment", "Value": "{environment}"},
                {"Key": "ProjectName", "Value": "Data Lake"}, {
                    "Key": "TechnicalTeam", "Value": "Arquitetura"}
            ],
            "BootstrapActions": [
                {
                    "Name": "Install Python Libs",
                    "ScriptBootstrapAction": {
                        "Path": "s3://256240406578-datalake-{environment}-raw/codes/emr-jobs/scripts/bootstrap_install_emr_cluster.sh"
                    }
                }
            ],
            "StepConcurrencyLevel": 4,

            "AutoTerminationPolicy": {
                "IdleTimeout": 120
            }
        },
        aws_conn_id="aws_default"
    )

    # Wait running Emr Cluster
    wait_for_emr = EmrJobFlowSensor(
        task_id='wait_for_emr',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states=['WAITING'],
        aws_conn_id='aws_default'
    )

    # submit_spark_step = EmrAddStepsOperator(
    #     task_id=STEP_TASK_ID,
    #     job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
    #     steps=SPARK_STEPS,
    #     aws_conn_id='aws_default'

    # )
    # last_step = len(SPARK_STEPS) - 1

    my_step_operator = EMRStepOperator(
    task_id='my_step_operator',
    job_flow_id='j-XXXXXXXXXXXXX',
    steps=steps,
    wait_for_completion=True
    )


    # Wait executions of all steps
    wait_for_steps_completion = EmrStepSensor(
        task_id='wait_for_steps_completion',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
        step_id= None,
        aws_conn_id='aws_default'
    )

    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
    )

    # Only display - end_dag
    end_dag = DummyOperator(task_id="end_dag")

    # Data pipeline flow
    start_dag >> create_emr_cluster
    create_emr_cluster >> wait_for_emr
    wait_for_emr >> submit_spark_step
    submit_spark_step >> wait_for_steps_completion
    wait_for_steps_completion >> terminate_emr_cluster
    terminate_emr_cluster >> end_dag


"""
1 - 
Configurar os blocos de tamanho do arquivo:
Configure o tamanho dos blocos do arquivo para corresponder ao tamanho do bloco padrão do S3, que é de 128 MB.
Isso ajuda a minimizar a quantidade de transferências de dados e minimizar o número de solicitações ao S3.
Para configurar isso, você pode definir a seguinte propriedade:

spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", "134217728")

2 -
Configurar o número de conexões por host: O S3 suporta apenas um número limitado de conexões por host.
Para otimizar a performance, você pode configurar o número máximo de conexões por host.
Para configurar isso, você pode definir a seguinte propriedade:

spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")

3 -
Configurar o número de partições:
O número de partições determina o número de tarefas que o Spark vai criar para ler ou gravar os dados.
O número de partições deve ser configurado com base no tamanho do arquivo e no tamanho do cluster.
Em geral, é recomendado ter um número de partições igual ao número de núcleos no cluster.
Para configurar isso, você pode definir a seguinte propriedade:

spark.conf.set("spark.sql.shuffle.partitions", "100")

4 -
Tamanho do bloco de dados: o Spark SQL lê e grava dados em blocos, assim como o Hadoop MapReduce.
Você pode ajustar o tamanho do bloco de dados para otimizar a performance de leitura e gravação de dados no S3.
Para configurar isso, você pode definir a seguinte propriedade:

spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")

5 - 
Paralelismo de leitura: o Spark SQL lê dados em paralelo, assim como o Spark em geral.
Você pode ajustar o paralelismo de leitura para otimizar a performance de leitura de dados no S3.
Para configurar isso, você pode definir a seguinte propriedade:

spark.conf.set("spark.sql.files.openCostInBytes", "8388608")
"""
