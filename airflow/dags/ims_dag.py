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
from airflow import configuration

args = {
    'owner': 'xyz',
    'email': ['guptakumartanuj@gmail.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='IMS_DAG_xyz', default_args=args,
    schedule_interval='@once')

dag.doc_md = __doc__
data = {}
data['client_id'] = configuration.get('ims', 'client_id').encode('utf-8')
data['grant_type'] = configuration.get('ims', 'grant_type').encode('utf-8') 
data['client_secret'] = configuration.get('ims', 'client_secret').encode('utf-8')
data['code'] = configuration.get('ims', 'code').encode('utf-8')

def response_check(response):
    """
    Dumps the http response and returns True when the http call status is 200/success
    """
    print(response)
    print(response.text)
    return response.status_code == 200  
    
t2 = SimpleHttpOperator(
    task_id='IMS_Token',
    #task_id='Lookup_Post',
    http_conn_id='ims_default',
    #http_conn_id='lookup_conn',
    method='POST',
    #data=json.dumps({"sql" : "SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production a, colors_production b WHERE a.color_name = b.color_name LIMIT 5;"}),
    #data=json.dumps(data),
    data={"grant_type":"authorization_code","client_id":"MCDPCatalogServiceQa2","client_secret":"c7d5585d-12be-4aed-b7ce-1fb628772f12","code":"eyJhbGciOiJSUzI1NiIsIng1dSI6Imltcy1rZXktMS5jZXIifQ.eyJpZCI6Ik1DRFBDYXRhbG9nU2VydmljZVFhMl9kZXZxYSIsInNjb3BlIjoic3lzdGVtLEFkb2JlSUQsb3BlbmlkIiwic3RhdGUiOm51bGwsImFzIjpudWxsLCJjcmVhdGVkX2F0IjoiMTQ0Nzc3MjY2OTEyMCIsImV4cGlyZXNfaW4iOiIyNTkyMDAwMDAwMDAiLCJ1c2VyX2lkIjoiTUNEUENhdGFsb2dTZXJ2aWNlUWEyQEFkb2JlSUQiLCJjbGllbnRfaWQiOiJNQ0RQQ2F0YWxvZ1NlcnZpY2VRYTIiLCJ0eXBlIjoiYXV0aG9yaXphdGlvbl9jb2RlIn0.Xhy8j9higZlPl7bDOdERMNzMdv9WbSLou6afxUuJ4mJJUFgqzFy0xOyqyxOS8_26LTAXCyTFVQBd75k6bmIGJvVXvyVLPdFuDV0A0eaJQIZgqWolGLQdSYJfiuVlFRwhWoNmQT6an6NbYNgqf4ozv4evidC0U8PYoRChejfyNObTF913O5EcqQJTT6-jAnr4xDqx66LB9u7iSv6RTwG7G7IIxtePa2ZWzHwcB_L5VunzTOJuVVuNZ7lmifE_ZH7e6Up6UOkcPFJeMWUgNg3Y9VWJdUkFkXsbuh6k-stj0QacRRUsqQRDaYyuK3P_kq6egpJeTJinEZqXzpU4KfcCEA"},
    #data=json.dumps({'ldap' : 'tangupta'}),
    endpoint='',
    #endpoint='/lookup/dataSets/testCollection/keys/ldap?imsOrg=testDb',
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    #headers={"Content-Type": "application/json"},
    xcom_push=True,
    response_check=response_check,
    dag=dag)

def print_hello():
    return 'Hello world!'

#from IPython import embed; embed()

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

t2.set_upstream(hello_operator)