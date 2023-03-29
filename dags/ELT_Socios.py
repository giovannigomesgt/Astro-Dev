from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import os
import requests
from zipfile import ZipFile
import re
from bs4 import BeautifulSoup
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from urllib.parse import urljoin
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pathlib

# Variables

ENVIRONMENT = Variable.get("ENVIRONMENT")
OBJECT = 'Socios'
BUCKET_NAME = f'256240406578-datalake-{ENVIRONMENT}-raw'
PREFIX_OBJECT = f'dados_publicos_cnpj/{OBJECT}/'
RF_URL = 'https://dadosabertos.rfb.gov.br/CNPJ/'

# Cria um objeto S3Hook
s3_hook = S3Hook(aws_conn_id='aws_default')

def getobject():
    soup = BeautifulSoup(requests.get(RF_URL).content, 'html.parser')
    links = [urljoin(RF_URL, link.get('href'))
             for link in soup.find_all('a', href=re.compile('\.zip$'))]
    for link in links:
        print(link)

    filescnaes = [lista for lista in links if OBJECT.lower() in lista.lower()]
    return filescnaes


def getVersionS3():
    try:
        keys = s3_hook.list_keys(BUCKET_NAME, prefix=PREFIX_OBJECT)
        if len(keys) > 1:
            firstObject = keys[0]
            response = s3_hook.get_key(firstObject, BUCKET_NAME)

            # Imprime a data da última modificação
            last_modified = response.last_modified.replace(tzinfo=None)
            print(f'last_modified do arquivo {last_modified}')
            # CONVERTENDO datetime.datetime para str
            print(type(last_modified.strftime("%d/%m/%Y %H:%M:%S")))
            return last_modified.strftime("%d/%m/%Y %H:%M:%S")
        else:
            print(type('01/01/2000 00:00:00'))
            return '01/01/2000 00:00:00'

    except Exception as e:
        if 'AccessDenied' in str(e):
            # tratamento de exceção específico para permissões insuficientes
            print(
                f'Não foi possível acessar o bucket: {BUCKET_NAME} de {ENVIRONMENT} devido a permissões insuficientes')
            return None

        elif '404' in str(e):
            print('Arquivo não encontrado')
            return None
        else:
            return None


def get_rf_versions():
    page = requests.get(RF_URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table')
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if cells:
            file_name = cells[1].text.replace(' ', '')
            if '.zip' in file_name.lower() and OBJECT.lower() in file_name.lower():
                # Remove caracteres não numéricos
                data_hora_str = re.sub(r'[^\d]+', '', cells[2].text)

                data_hora = datetime.strptime(data_hora_str, '%Y%m%d%H%M%S')
                # <class 'datetime.datetime'>
                print(data_hora.strftime("%d/%m/%Y %H:%M:%S"))

                # print(type(date_obj.strftime("%d/%m/%Y %H:%M:%S")))
                return (data_hora.strftime("%d/%m/%Y %H:%M:%S"))

    return None


def versionamento(task_instance):
    rf_version = task_instance.xcom_pull(
        task_ids=f'Checking_{OBJECT}_Version_on_web')
    s3_version = task_instance.xcom_pull(
        task_ids=f'Checking_{OBJECT}_Version_on_s3')

    date_format = '%d/%m/%Y %H:%M:%S'

    rf_version_date_obj = datetime.strptime(rf_version, date_format)
    s3_version_date_obj = datetime.strptime(s3_version, date_format)

    if rf_version > s3_version:
        print('DADOS DO GOV ESTÃO MAIS ATUALIZADOS!')
        print('')
        print(
            f'VERSÃO NO SITE OFICIAL {rf_version_date_obj.strftime("%d/%m/%Y %H:%M:%S")}')
        print(
            f'VERSÃO NO S3 {s3_version_date_obj.strftime("%d/%m/%Y %H:%M:%S")}')
        return 'Downloading'
    else:
        print(
            f'VERSÃO NO SITE OFICIAL {rf_version_date_obj.strftime("%d/%m/%Y %H:%M:%S")}')
        print(
            f'VERSÃO NO S3 {s3_version_date_obj.strftime("%d/%m/%Y %H:%M:%S")}')
        return 'Updated_files'


def download(task_instance):
    file_urls = task_instance.xcom_pull(task_ids=f'Get_links_{OBJECT}')
    for file_url in file_urls:
        response = requests.head(file_url)
        file_name = os.path.basename(file_url)
        folder_name = os.path.splitext(file_name)[0]

        endereco = re.sub(r'\d+', '', folder_name)
        os.makedirs(endereco, exist_ok=True)

        checkfiles = os.listdir(endereco)
        if f"{folder_name}.zip" in checkfiles or f"{folder_name}.CSV" in checkfiles:
            print('Arquivo já existe')
        else:
            response = requests.get(file_url, stream=True)
            print('*' * 100)
            print(f"Downloading {file_name}")
            print('*' * 100)
            with open(f'{endereco}/{file_name}', "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            print('*' * 100)
            print('Download Concluido!')
            print('*' * 100)
    return f'{endereco}/{file_name}'


def skip():
    print('*'*50)
    print('A versão do arquivo está ok')
    print('*'*50)
    return 'Finish'


def extract():
    folder_path = OBJECT
    print('*'*150)
    print('Iniciando Extração')
    print('*'*150)
    files = []

    if not os.path.exists(folder_path):
        print('*'*50)
        print('Pasta não encontrada')
        print('*'*50)
        return files

    for file in os.listdir(folder_path):
        if file.endswith('.zip'):
            file_path = os.path.join(folder_path, file)
            print(file_path)

            with ZipFile(file_path, "r") as zip_file:
                for compressed_file in zip_file.namelist():
                    # print(compressed_file)
                    zip_file.extract(compressed_file, folder_path)

                print(f'{file_path} extraído')
                os.remove(file_path)
                print(f'{file_path} excluído')

                file_name = os.path.splitext(file)[0] + '.CSV'
                new_file_path = os.path.join(folder_path, file_name)

                try:
                    os.rename(os.path.join(
                        folder_path, compressed_file), new_file_path)
                    print(
                        f'Arquivo {compressed_file} renomeado para {file_name}')
                except FileExistsError:
                    print('Arquivo já existe')

                files.append(file_name)

    print('*'*50)
    return files


def uploadS3():
    folder_path = OBJECT
    if os.path.exists(folder_path):
        folder = pathlib.Path(folder_path)
        for file in folder.glob('**/*'):
            if file.is_file():
                print('*' * 150)
                print(f'ARQUIVO ATUAL: {file.name}')

                print(f"Enviando {file} para o bucket {BUCKET_NAME} endereço da pasta: {PREFIX_OBJECT}{file.name}")

                s3_hook.load_file(
                    filename=file,
                    key=f'{PREFIX_OBJECT}{file.name}',
                    bucket_name=BUCKET_NAME,
                    replace=True
                )
                print(f'Arquivo {file.name} Enviado!')
                print(f'Excluindo {file}')
                os.remove(file)
                print('*' * 150)
    else:
        print(f"O caminho {folder_path} não existe")


with DAG(f'ELT_{OBJECT}', start_date=datetime(2022, 12, 16), schedule_interval=None, catchup=False, tags=['VERSIONAMENTO', 'S3']) as dag:

    Start = DummyOperator(task_id="Start")
    Finish = DummyOperator(
        task_id="Finish", trigger_rule=TriggerRule.NONE_FAILED)

    taskgetlink = PythonOperator(
        task_id=f'Get_links_{OBJECT}',
        python_callable=getobject
    )

    taskversionings3 = PythonOperator(
        task_id=f'Checking_{OBJECT}_Version_on_s3',
        python_callable=getVersionS3
    )

    taskversioningrf = PythonOperator(
        task_id=f'Checking_{OBJECT}_Version_on_web',
        python_callable=get_rf_versions
    )

    taskversion = BranchPythonOperator(
        task_id='Checking_Objects_Version',
        python_callable=versionamento,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    taskdownload = PythonOperator(
        task_id='Downloading',
        python_callable=download
    )

    taskskip = BranchPythonOperator(
        task_id='Updated_files',
        python_callable=skip
    )

    taskextract = PythonOperator(
        task_id='Extracting',
        python_callable=extract
    )

    taskupload = PythonOperator(
        task_id='Uploading_files',
        python_callable=uploadS3
    )


# ORQUESTRAÇÃO
Start >> taskgetlink
taskgetlink >> [taskversionings3, taskversioningrf]
[taskversionings3, taskversioningrf] >> taskversion
taskversion >> taskdownload >> taskextract >> taskupload >> Finish
taskversion >> taskskip >> Finish
