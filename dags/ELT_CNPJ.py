from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import os
import requests
from zipfile import ZipFile
import re
import json
from bs4 import BeautifulSoup
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timezone
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from urllib.parse import urljoin
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from multiprocessing.pool import ThreadPool
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor, EmrJobFlowSensor


s3_client = S3Hook(aws_conn_id='aws_default')

TYPE = "batch"
MODE = "historical"
EVENT = "dadosgov"

# Variables
ENVIRONMENT = Variable.get("ENVIRONMENT")
CONF_CLUSTER = Variable.get("path_conf_cluster")
BUCKET_NAME_SCRIPTS = f'256240406578-datalake-{ENVIRONMENT}-raw'
RF_URL = 'https://dadosabertos.rfb.gov.br/CNPJ/'
S3_SCRIPT_CNPJ_ETL_REFINED_MODEL = "codes/emr-jobs/refined/cnpjgov_model.py"


DAG_ID = f"{EVENT}_model_{ENVIRONMENT}_{MODE}"
STEP_TASK_ID = f"add_steps_{EVENT}_model"  
TAGS = ["DataLake", "EMR", f"AWS{ENVIRONMENT.upper()}EMR", EVENT.capitalize(), 'Versionamento', 'CNPJ','S3','EMR','GOV']


# TASKS VARIABELS
STEP_PARAMS = {
    "BUCKET_NAME": BUCKET_NAME_SCRIPTS,
    "S3_SCRIPT": S3_SCRIPT_CNPJ_ETL_REFINED_MODEL
}

with open(CONF_CLUSTER)  as conf_cluster:
    JOB_FLOW_OVERRIDES_STR = conf_cluster.read()
    JOB_FLOW_OVERRIDES_STR = JOB_FLOW_OVERRIDES_STR.replace('{type}',TYPE)\
                                                    .replace('{event}',EVENT)\
                                                    .replace('{mode}', MODE)\
                                                    .replace('{emr_version}','emr-6.9.0')\
                                                    .replace('{environment}',ENVIRONMENT)  \
                                                    .replace('{market}','ON_DEMAND' if ENVIRONMENT == 'prd' else 'SPOT')  \
                                                    .replace('{master_instance_type}','m4.large')  \
                                                    .replace('{worker_instance_type}','r5a.xlarge')  \
                                                    .replace('"{worker_instance_count}"','3')  \
                                                    .replace('{label}',f'AWS{ENVIRONMENT.upper()}EMR') \
                                                    .replace('"{step_concurrency_level}"','1')      
    JOB_FLOW_OVERRIDES = json.loads(JOB_FLOW_OVERRIDES_STR)


# Set default arguments
default_args = {
                    "owner": "EMR-Airflow",
                    "depends_on_past": False,
                    "start_date": datetime(2023, 4, 24),
                    "email": ["airflow@airflow.com"],
                    "email_on_failure": False,
                    "email_on_retry": False
                }

# SETTING SCHEDULER
if ENVIRONMENT == "prd":
        SCHEDULER = "0 0 * * *"
else:
    SCHEDULER = None


def getobject(rf_url):
    linksreturn = {}
    
    soup = BeautifulSoup(requests.get(rf_url).content, 'html.parser')
    links = [urljoin(rf_url, link.get('href'))
             for link in soup.find_all('a', href=re.compile('\.zip$'))]
    
    linksgov = [(re.sub(r'\d','',i.split('/')[-1].split('.')[0]), i) for i in links]

    for i in linksgov:
        if i[0] in linksreturn:
            linksreturn[i[0]][0].append(i[1])
        else:
            linksreturn[i[0]] = [[i[1]],f'dados_publicos_cnpj/{i[0]}/',False]
    

    return linksreturn

def getVersionS3(bucket, object):
    keys = s3_client.list_keys(bucket, prefix=object)
    
    if len(keys) >= 1:
        firstObject = keys[-1]
        response = s3_client.get_key(firstObject, bucket)

        # Imprime a data da última modificação
        last_modified = response.last_modified.replace(tzinfo=None)
        return last_modified

    else:
        return datetime(1999, 1, 1)

def get_rf_versions():
    versoes = {}
    page = requests.get(RF_URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table')
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if cells:
            if '.zip' in cells[1].text:
                name = re.sub(r'\d','',cells[1].text.strip('.zip '))
                #data_hora_str = re.sub(r'[^\d]+', '', cells[2].text)
                versoes[name] = datetime.strptime(f'{cells[2].text.strip(" ")}:00', '%Y-%m-%d %H:%M:%S')

    #print(versoes)
    return versoes

def filesForDownload():
    links = getobject(RF_URL)
    versionS3 = {k: getVersionS3(BUCKET_NAME_SCRIPTS, v[1]) for k,v in links.items()}
    versionUrl = get_rf_versions()
    for k, v in links.items():
        if versionUrl[k] > versionS3[k]:
            v[2] = True

    return {k : v for k , v in links.items() if v[2]}

def check_file_versions():
    files = filesForDownload()
    if files:
        return 'Files_For_Download'
    else:
        return 'Finish'

def download(ds, **kwargs):
    # obtém a instância da tarefa atual
    task_instance = kwargs['task_instance']
    results = {}
    # busca o valor retornado pela outra tarefa
    file_urls = task_instance.xcom_pull(task_ids='Files_For_Download', key='return_value')
    for k, v in file_urls.items():
        print(f'DOWNLOADING {k}')
        pool = ThreadPool(len(v[0]))
        result = pool.map(runDownloadFiles, v[0])
        pool.close()
        pool.join()
        #result = pool.get_results()
        results[k] = result
    return results

def runDownloadFiles(file_url):
    response = requests.head(file_url)
    file_name = os.path.basename(file_url)
    folder_name = os.path.splitext(file_name)[0]

    endereco = re.sub(r'\d+', '', folder_name)
    os.makedirs(endereco, exist_ok=True)

    checkfiles = os.listdir(endereco)
    if f"{folder_name}.zip" in checkfiles or f"{folder_name}.CSV" in checkfiles:
        print('Arquivo já existe')
        os.remove(f'{endereco}/{file_name}')

    
    response = requests.get(file_url, stream=True)
    print('*' * 100)
    print(f"Downloading {file_name}")
    
    with open(f'{endereco}/{file_name}', "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    
    print(f'Download {file_name} Concluido!')
    print('*' * 100)

    return f'{endereco}/{file_name}'

def extract(file):
    folder_path = os.path.dirname(file)

    print('*'*150)
    print('Iniciando Extração')
    print('*'*150)

    newName = file.split("/")[-1].replace(".zip",".csv")
    newNamePath = f'{folder_path}/{newName}'

    with ZipFile(file, "r") as zip_file:
        for compressed_file in zip_file.namelist():
            print(compressed_file)
            zip_file.extract(compressed_file, folder_path)

        os.rename(os.path.join(
            folder_path, compressed_file), newNamePath)
        print(
            f'Arquivo {compressed_file} renomeado para {newNamePath}')
        
    os.remove(file)
    print('*'*50)

    return newNamePath

def extracAll(ds, **kwargs):
    # obtém a instância da tarefa atual
    task_instance = kwargs['task_instance']
    results = []
    # busca o valor retornado pela outra tarefa
    files_zip = task_instance.xcom_pull(task_ids='Downloading', key='return_value')
    #files_zip = arquivos0

    for k, v in files_zip.items():
        print(f'EXTRAINDO {k}')
        pool = ThreadPool(len(v))
        result = pool.map(extract, v)
        pool.close()
        pool.join()
        #result = pool.get_results()
        results.extend(result)
    return results

def uploadS3(path):
    print('*' * 150)
    print(f'ARQUIVO ATUAL: {path}')

    print(f"Enviando {path} para o bucket {BUCKET_NAME_SCRIPTS} endereço da pasta: dados_publicos_cnpj/{path}")

    s3_client.load_file(
        filename=path,
        key=f'dados_publicos_cnpj/{path}',
        bucket_name=BUCKET_NAME_SCRIPTS,
        replace=True
    )
    print(f'Arquivo {path} Enviado!')
    print(f'Excluindo {path}')
    os.remove(path)
    print('*' * 150)

def uploadS3All(ds, **kwargs):
    task_instance = kwargs['task_instance']
    file = task_instance.xcom_pull(task_ids='Extract', key='return_value')
    pool = ThreadPool(10)
    pool.map(uploadS3, file)
    pool.close()
    pool.join()
    return file

def emrStepsCreator(ds, **kwargs):
    TABLES = []
    task_instance = kwargs['task_instance']
    file = task_instance.xcom_pull(task_ids='Files_For_Download', key='return_value')
    if file:
        for k, v in file.items():
            TABLES.append(k)
            
        SPARK_STEPS = [{
            'Name': 'Processa dados do Gov',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                'spark-submit',
                's3://{{params.BUCKET_NAME}}/{{params.S3_SCRIPT}}', ENVIRONMENT, ','.join(TABLES)
                    ]
                }
            }]

    return SPARK_STEPS
#f's3://{BUCKET_NAME_SCRIPTS }/{S3_SCRIPT_CNPJ_ETL_REFINED_MODEL}', ENVIRONMENT, TABLES

def get_steps_return(**context):
    ti = context['ti']
    add_steps_result = ti.xcom_pull(task_ids=STEP_TASK_ID)
    return add_steps_result[-1]


with DAG(
    DAG_ID,
    default_args = default_args,
    start_date=datetime(2023, 4, 24),
    schedule_interval=SCHEDULER,
    catchup=False,
    tags=TAGS) as dag:

    Start = DummyOperator(task_id="Start")
    Finish = DummyOperator(
        task_id="Finish", trigger_rule=TriggerRule.NONE_FAILED)

    taskversion = BranchPythonOperator(
        task_id = 'Check_File_Versions',
        python_callable = check_file_versions
    )

    taskgetfiles = PythonOperator(
        task_id=f'Files_For_Download',
        python_callable=filesForDownload
    )

    taskdownload = PythonOperator(
        task_id='Downloading',
        provide_context=True,
        python_callable=download
    )

    taskextract = PythonOperator(
        task_id = 'Extract',
        provide_context=True,
        python_callable=extracAll
    )

    taskupload = PythonOperator(
        task_id='Uploading_files',
        provide_context=True,
        python_callable=uploadS3All
    )

    taskstepscreator = PythonOperator(
        task_id='CreatingStepsEmr',
        provide_context=True,
       python_callable=emrStepsCreator
    )

    ################ EMR ################
    # Create the EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides = JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default"
    )

    # Wait running Emr Cluster
    wait_for_emr = EmrJobFlowSensor(
        task_id='wait_for_emr',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states=['WAITING'],
        aws_conn_id='aws_default'
    )

    # Add the steps to the EMR cluster
    add_steps = EmrAddStepsOperator(
        task_id = STEP_TASK_ID,
        job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id = "aws_default",
        steps = "{{ task_instance.xcom_pull(task_ids='CreatingStepsEmr', key='return_value') }}",
        params = STEP_PARAMS
        )
       
    # Wait executions of all steps
    wait_for_steps_completion = EmrStepSensor(
        task_id='wait_for_steps_completion',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
        step_id = f"{'{{'} task_instance.xcom_pull(task_ids='add_steps_{EVENT}_model', key='return_value')["
                        + str(-1)
                        + "] }}",
        aws_conn_id='aws_default'
    )
    
    # Terminate the EMR cluster
    Finalizing_EMR_Cluster = EmrTerminateJobFlowOperator(
        task_id="Finalizing_EMR_Cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value')}}",
        aws_conn_id="aws_default"
    )



#ORQUESTRAÇÃO
Start >> taskversion >> Finish
taskversion >> taskgetfiles
taskversion  >> taskgetfiles >> taskdownload
taskgetfiles >> taskstepscreator >> add_steps
taskdownload >> taskextract >> taskupload
taskupload >> create_emr_cluster 
create_emr_cluster >> wait_for_emr
wait_for_emr >> add_steps
add_steps >> wait_for_steps_completion
wait_for_steps_completion >> Finalizing_EMR_Cluster
Finalizing_EMR_Cluster >> Finish