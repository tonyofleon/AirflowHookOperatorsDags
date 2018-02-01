# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
### Example HTTP operator and sensor
"""
import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import HttpSensor
from datetime import timedelta
import json
import MySQLdb
import logging;
from airflow.hooks.http_hook import HttpHook
import os
from airflow import configuration

args = {
    'owner': 'xyz',
    'email': ['guptakumartanuj@gmail.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

task=json.dumps({"sql":"SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production a, colors_production b WHERE a.color_name = b.color_name LIMIT 5;"})

dag = DAG(
    dag_id='Compute_Gatewat_xyz_Dag', default_args=args,
    schedule_interval='@once')

dag.doc_md = __doc__

def response_check(response):
    """
    Dumps the http response and returns True when the http call status is 200/success
    """
    print(response)
    print(response.text)
    return response.status_code == 201  

rq_body_param = {
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
    
def response_check(response):
    """
    Dumps the http response and returns True when the http call status is 200/success
    """
    print(response)
    print(response.text)
    return response.status_code == 200  
 
#   ERROR - Type '<type 'int'>' used for parameter 'data[driverMemory]' is not supported for templating  
#t1 = SimpleHttpOperator(
#    task_id='IMS_Token',
    #task_id='Lookup_Post',
#   http_conn_id='ims_default',
    #http_conn_id='lookup_conn',
#    method='POST',
    #data=json.dumps({"sql" : "SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production a, colors_production b WHERE a.color_name = b.color_name LIMIT 5;"}),
#    data={"grant_type":"authorization_code","client_id":"MCDPCatalogServiceQa2","client_secret":"c7d5585d-12be-4aed-b7ce-1fb628772f12","code":"eyJhbGciOiJSUzI1NiIsIng1dSI6Imltcy1rZXktMS5jZXIifQ.eyJpZCI6Ik1DRFBDYXRhbG9nU2VydmljZVFhMl9kZXZxYSIsInNjb3BlIjoic3lzdGVtLEFkb2JlSUQsb3BlbmlkIiwic3RhdGUiOm51bGwsImFzIjpudWxsLCJjcmVhdGVkX2F0IjoiMTQ0Nzc3MjY2OTEyMCIsImV4cGlyZXNfaW4iOiIyNTkyMDAwMDAwMDAiLCJ1c2VyX2lkIjoiTUNEUENhdGFsb2dTZXJ2aWNlUWEyQEFkb2JlSUQiLCJjbGllbnRfaWQiOiJNQ0RQQ2F0YWxvZ1NlcnZpY2VRYTIiLCJ0eXBlIjoiYXV0aG9yaXphdGlvbl9jb2RlIn0.Xhy8j9higZlPl7bDOdERMNzMdv9WbSLou6afxUuJ4mJJUFgqzFy0xOyqyxOS8_26LTAXCyTFVQBd75k6bmIGJvVXvyVLPdFuDV0A0eaJQIZgqWolGLQdSYJfiuVlFRwhWoNmQT6an6NbYNgqf4ozv4evidC0U8PYoRChejfyNObTF913O5EcqQJTT6-jAnr4xDqx66LB9u7iSv6RTwG7G7IIxtePa2ZWzHwcB_L5VunzTOJuVVuNZ7lmifE_ZH7e6Up6UOkcPFJeMWUgNg3Y9VWJdUkFkXsbuh6k-stj0QacRRUsqQRDaYyuK3P_kq6egpJeTJinEZqXzpU4KfcCEA"},
    #data=json.dumps({'ldap' : 'tangupta'}),
#    endpoint='',
    #endpoint='/lookup/dataSets/testCollection/keys/ldap?imsOrg=testDb',
#   headers={"Content-Type": "application/x-www-form-urlencoded"},
    #headers={"Content-Type": "application/json"},
#    xcom_push=True,
#    response_check=response_check,
#    dag=dag)
    
t2 = SimpleHttpOperator(
    task_id='Compute',
    #task_id='Lookup_Post',
    http_conn_id='cg_default',
    #http_conn_id='lookup_conn',
    method='POST',
    #data=json.dumps({"sql" : "SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production a, colors_production b WHERE a.color_name = b.color_name LIMIT 5;"}),
    data=rq_body_param,
    #data=json.dumps({'ldap' : 'tangupta'}),
    endpoint='data/foundation/compute/jobs',
    #endpoint='/lookup/dataSets/testCollection/keys/ldap?imsOrg=testDb',
    headers={"Content-Type": "application/json", "accept": "application/json", "x-api-key": "acp_testing", "Authorization" : "Bearer eyJ4NXUiOiJpbXNfbmExLXN0ZzEta2V5LTEuY2VyIiwiYWxnIjoiUlMyNTYifQ.eyJpZCI6IjE1MTcyMTI4MjAzMjhfNjNkMzI5NjMtOTYzYy00YjA2LTk3MjAtN2M2OTExZDI2Y2E5X3VlMSIsImNsaWVudF9pZCI6ImFjcF90ZXN0aW5nIiwidXNlcl9pZCI6ImFjcF90ZXN0aW5nQEFkb2JlSUQiLCJ0eXBlIjoiYWNjZXNzX3Rva2VuIiwiYXMiOiJpbXMtbmExLXN0ZzEiLCJwYWMiOiJhY3BfdGVzdGluZ19zdGciLCJydGlkIjoiMTUxNzIxMjgyMDMyOV84YzFhYzRhOC1lZjM0LTQ3ZWYtOWFkNi0xMmI0ZTg3MjYzNjdfdWUxIiwicnRlYSI6IjE1MTg0MjI0MjAzMjkiLCJtb2kiOiJkMjVhMzg5ZSIsImMiOiJZSFg3Rld5d2JnaDhTYy9FMW1vaWJBPT0iLCJleHBpcmVzX2luIjoiODY0MDAwMDAiLCJzY29wZSI6ImFjcC5mb3VuZGF0aW9uLmFjY2Vzc0NvbnRyb2wsYWNwLmNvcmUucGlwZWxpbmUsc3lzdGVtLG9wZW5pZCxBZG9iZUlELGFkZGl0aW9uYWxfaW5mby5yb2xlcyxhZGRpdGlvbmFsX2luZm8ucHJvamVjdGVkUHJvZHVjdENvbnRleHQsYWNwLmZvdW5kYXRpb24sYWNwLmZvdW5kYXRpb24uY2F0YWxvZyxhY3AuZGlzY292ZXJ5IiwiY3JlYXRlZF9hdCI6IjE1MTcyMTI4MjAzMjgifQ.Q0eAxwLdkQ7XEDzpVwDtoKsmwySkEN26F85wDWjgo5j8lriO_8hUDEYYTXJjvXd0xOr82OnIQnWrDe8LXGLswH2rUYmR0oC40Wfv_ZMLf6IPyghNSw5QWKMYhOKTq-4n2kFvnvSh2Dq_F3govWSo1OWR609xC-HKLGAfBgWqAvCN5WPGQzQ8e5zeqCgclBTk4noBqJIVV06hJROSiD2Gt7FyC6YNMm3B-fVaOfFb4C2WBeGprQphXsVirMSvt9lWEYKqo5pGHgOlL5U40LeWFQMcnfOcmIntDG56BE3lhdyQeeltYbZlg1_RwsVwL5OcVWCtceyB0PWj9HheqvRsvA"},
    #headers={"Content-Type": "application/json"},
    xcom_push=True,
    response_check=response_check,
    dag=dag)    

def print_hello():
    task_id='CG_details'
    #task_id='Lookup_Post',
    http_conn_id='cg_default'
    #http_conn_id='lookup_conn',
    method='POST'
    data=rq_body_param,
    #data={"sql":"SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production a, colors_production b WHERE a.color_name = b.color_name LIMIT 5;"},
    #data=json.dumps({'ldap' : 'tangupta'}),
    endpoint='xyz/test/execute/jobs'
    #endpoint='/lookup/dataSets/testCollection/keys/ldap?imsOrg=testDb',
    headers={"Content-Type": "application/json", "accept": "application/json", "x-api-key": "acp_testing", "Authorization" : "Bearer eyJ4NXUiOiJpbXNfbmExLXN0ZzEta2V5LTEuY2VyIiwiYWxnIjoiUlMyNTYifQ.eyJpZCI6IjE1MTcyMTI4MjAzMjhfNjNkMzI5NjMtOTYzYy00YjA2LTk3MjAtN2M2OTExZDI2Y2E5X3VlMSIsImNsaWVudF9pZCI6ImFjcF90ZXN0aW5nIiwidXNlcl9pZCI6ImFjcF90ZXN0aW5nQEFkb2JlSUQiLCJ0eXBlIjoiYWNjZXNzX3Rva2VuIiwiYXMiOiJpbXMtbmExLXN0ZzEiLCJwYWMiOiJhY3BfdGVzdGluZ19zdGciLCJydGlkIjoiMTUxNzIxMjgyMDMyOV84YzFhYzRhOC1lZjM0LTQ3ZWYtOWFkNi0xMmI0ZTg3MjYzNjdfdWUxIiwicnRlYSI6IjE1MTg0MjI0MjAzMjkiLCJtb2kiOiJkMjVhMzg5ZSIsImMiOiJZSFg3Rld5d2JnaDhTYy9FMW1vaWJBPT0iLCJleHBpcmVzX2luIjoiODY0MDAwMDAiLCJzY29wZSI6ImFjcC5mb3VuZGF0aW9uLmFjY2Vzc0NvbnRyb2wsYWNwLmNvcmUucGlwZWxpbmUsc3lzdGVtLG9wZW5pZCxBZG9iZUlELGFkZGl0aW9uYWxfaW5mby5yb2xlcyxhZGRpdGlvbmFsX2luZm8ucHJvamVjdGVkUHJvZHVjdENvbnRleHQsYWNwLmZvdW5kYXRpb24sYWNwLmZvdW5kYXRpb24uY2F0YWxvZyxhY3AuZGlzY292ZXJ5IiwiY3JlYXRlZF9hdCI6IjE1MTcyMTI4MjAzMjgifQ.Q0eAxwLdkQ7XEDzpVwDtoKsmwySkEN26F85wDWjgo5j8lriO_8hUDEYYTXJjvXd0xOr82OnIQnWrDe8LXGLswH2rUYmR0oC40Wfv_ZMLf6IPyghNSw5QWKMYhOKTq-4n2kFvnvSh2Dq_F3govWSo1OWR609xC-HKLGAfBgWqAvCN5WPGQzQ8e5zeqCgclBTk4noBqJIVV06hJROSiD2Gt7FyC6YNMm3B-fVaOfFb4C2WBeGprQphXsVirMSvt9lWEYKqo5pGHgOlL5U40LeWFQMcnfOcmIntDG56BE3lhdyQeeltYbZlg1_RwsVwL5OcVWCtceyB0PWj9HheqvRsvA"}
    extra_options={}
    http = HttpHook(method, http_conn_id='cg_default')

    logging.info('Calling HTTP method')
    print(os.environ['PATH'])
    response = http.run(endpoint,
                            data,
                            headers,
                            extra_options)
    print(response)
    print(response.text)
    print(configuration.get('testing', 'tanuj').encode('utf-8'))
    return 'Hello world!'

#from IPython import embed; embed()

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

#t1.set_upstream(t2)
t2.set_upstream(hello_operator)