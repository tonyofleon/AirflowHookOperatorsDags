 #
 # xyz CONFIDENTIAL
 # __________________
 # Copyright 2017 xyz Systems Incorporated
 # All Rights Reserved.
 #
 # NOTICE:  All information contained herein is, and remains
 # the property of xyz Systems Incorporated and its suppliers,
 # if any.  The intellectual and technical concepts contained
 # herein are proprietary to xyz Systems Incorporated and its
 # suppliers and are protected by all applicable intellectual property laws,
 # including trade secret and copyright laws.
 #
 # Dissemination of this information or reproduction of this material
 # is strictly forbidden unless prior written permission is obtained
 # from xyz Systems Incorporated.
 #
 #
 #

import airflow

from airflow import DAG
from airflow.operators import ComputeGatewayOperator
from airflow.operators.http_operator import SimpleHttpOperator


args = {
    'owner': 'xyz',
    'email': ['guptakumartanuj@gmail.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='Compute_Dag', default_args=args,
    schedule_interval='@once')


body = {
  "name": "Wordcount",
  "sparkConf": {
  },
  "envVars": {
  },
  "className": "com.xyz.platform.batch.WordCount",
  "jar": "https://artifactory-uw2.xyzitc.com/artifactory/maven-spark-no-moonbeam-batch-example-release/com/xyz/platform/spark-no-moonbeam-batch-example/1.0/spark-no-moonbeam-batch-example-1.0.jar",
  "args": ["/usr/local/spark/README.md"],
  "driverMemory": 1024,
  "driverCores": 1,
  "executorMemory": 1024,
  "executorCores": 2,
  "numExecutors": 1
}

# Example of using the named parameters of ComputeGatewayOperator
# to initialize the operator.
compute_gateway_task = ComputeGatewayOperator(
    task_id='compute_gateway_task',
    dag=dag,
    json=body)

#ims_token_task.set_downstream(compute_gateway_task)
