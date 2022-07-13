from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataProcPySparkOperator, DataprocClusterDeleteOperator, DataProcJobBaseOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators import BashOperator, PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import os
import sys
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.ml.fpm import FPGrowth
import pyarrow.parquet as pq
#from dependencies.connectors.connector_gbq import ConnectorGBQ
from google.cloud import bigquery

# Airflow parameters
DEFAULT_DAG_ARGS = {
    'owner': 'airflow',  # The owner of the task.
    # Task instance should not rely on the previous task's schedule to succeed.
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,  # Retry once before failing the task.
    #'retry_delay': timedelta(minutes=5),  # Time between retries.
    'project_id': Variable.get('gcp_project')  # Cloud Composer project ID.

}


LIBS = {'CONDA_PACKAGES':'pandas-gbq google-cloud-core=1.3.0 six fsspec gcsfs','CONDA_CHANNELS':'conda-forge'}

 

# Create Directed Acyclic Graph for Airflow
with DAG('DAG_PROA_basket_analysis_process',
		default_args=DEFAULT_DAG_ARGS,
		schedule_interval='0 0 0 ? 1/3 MON *') as dag:
	
	create_cluster = DataprocClusterCreateOperator(
					task_id='create_dataproc_cluster',
					cluster_name='basket-analysis-spark-cluster',
					image_version='1.5-debian10',
					num_workers=2,
					#network_uri='https://www.googleapis.com/compute/v1/projects/lh-brlm/global/networks/lh-network',
					subnetwork_uri='https://www.googleapis.com/compute/v1/projects/lh-brlm/regions/us-central1/subnetworks/lh-subnet-usc1',
					region='us-central1',
					tags='all-ingress',
					custom_image_project_id='bi-prd-brlm',
					metadata=LIBS,
					init_actions_uris=['gs://goog-dataproc-initialization-actions-us-central1/python/conda-install.sh'],
					#custom_image='custom-dataproc-1-5-10-poc',
					#service_account=Variable.get('serviceAccount'),
					#storage_bucket=Variable.get('dataproc_bucket'),
					service_account_scopes='https://www.googleapis.com/auth/cloud-platform')
    
	basket_pyspark_task = DataProcPySparkOperator(
					task_id='basket-analysis',
					main='gs://analytics-proa-lmbr/scripts-data-science/basket_spark.py',
					cluster_name='basket-analysis-spark-cluster',
					region='us-central1',
					files='gs://analytics-proa-lmbr/scripts-data-science/dependencies/')

	cloud_storage_to_bigquery = GoogleCloudStorageToBigQueryOperator(task_id='gcs_to_bq',
					bucket='analytics-proa-lmbr',
					source_objects= ['Data/Basket-analysis-data/*.parquet'],
					destination_project_dataset_table='brlm-web-data.datalab.PROA_BASKET_ANALYSIS',
					autodetect= True,
					skip_leading_rows =1,
					create_disposition='CREATE_IF_NEEDED',
					write_disposition='WRITE_TRUNCATE',
					source_format='parquet')
	
	
	#Delete the Cloud Dataproc cluster.
	delete_cluster = DataprocClusterDeleteOperator(task_id='delete_dataproc_cluster',
					subnetwork_uri='https://www.googleapis.com/compute/v1/projects/lh-brlm/regions/us-central1/subnetworks/lh-subnet-usc1',
					region='us-central1',
					cluster_name='basket-analysis-spark-cluster',
					trigger_rule=TriggerRule.ALL_DONE)

create_cluster >> basket_pyspark_task >> cloud_storage_to_bigquery >> delete_cluster