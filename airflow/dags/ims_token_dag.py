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

import airflow
import json 

from airflow import DAG
from airflow.operators import IMSTokenOperator


# This is an example DAG which uses the QueryServiceOperator.
# In this example, we create two tasks which execute sequentially.
# The first task is to run a notebook at the workspace path "/test"
# and the second task is to run a JAR uploaded to DBFS. Both,
# tasks use new clusters.
#
# Because we have set a downstream dependency on the notebook task,
# the spark jar task will NOT run until the notebook task completes
# successfully.
#
# The definition of a succesful run is if the run has a result_state of "SUCCESS".
# For more information about the state of a run refer to
# https://docs.Query Service.com/api/latest/jobs.html#runstate

args = {
    'owner': 'xyz',
    'email': ['guptakumartanuj@gmail.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='IMS_Token_Dag', default_args=args,
    schedule_interval='@once')


ims_body_param ={"grant_type":"authorization_code","client_id":"MCDPCatalogServiceQa2","client_secret":"c7d5585d-12be-4aed-b7ce-1fb628772f12","code":"eyJhbGciOiJSUzI1NiIsIng1dSI6Imltcy1rZXktMS5jZXIifQ.eyJpZCI6Ik1DRFBDYXRhbG9nU2VydmljZVFhMl9kZXZxYSIsInNjb3BlIjoic3lzdGVtLEFkb2JlSUQsb3BlbmlkIiwic3RhdGUiOm51bGwsImFzIjpudWxsLCJjcmVhdGVkX2F0IjoiMTQ0Nzc3MjY2OTEyMCIsImV4cGlyZXNfaW4iOiIyNTkyMDAwMDAwMDAiLCJ1c2VyX2lkIjoiTUNEUENhdGFsb2dTZXJ2aWNlUWEyQEFkb2JlSUQiLCJjbGllbnRfaWQiOiJNQ0RQQ2F0YWxvZ1NlcnZpY2VRYTIiLCJ0eXBlIjoiYXV0aG9yaXphdGlvbl9jb2RlIn0.Xhy8j9higZlPl7bDOdERMNzMdv9WbSLou6afxUuJ4mJJUFgqzFy0xOyqyxOS8_26LTAXCyTFVQBd75k6bmIGJvVXvyVLPdFuDV0A0eaJQIZgqWolGLQdSYJfiuVlFRwhWoNmQT6an6NbYNgqf4ozv4evidC0U8PYoRChejfyNObTF913O5EcqQJTT6-jAnr4xDqx66LB9u7iSv6RTwG7G7IIxtePa2ZWzHwcB_L5VunzTOJuVVuNZ7lmifE_ZH7e6Up6UOkcPFJeMWUgNg3Y9VWJdUkFkXsbuh6k-stj0QacRRUsqQRDaYyuK3P_kq6egpJeTJinEZqXzpU4KfcCEA"}


# Example of using the JSON parameter to initialize the operator.
qs1_task = IMSTokenOperator(
    task_id='ims1_task',
    dag=dag,
    json={})

# Example of using the named parameters of QueryServiceOperator
# to initialize the operator.
qs2_task = IMSTokenOperator(
    task_id='ims2_task',
    dag=dag,
    json=ims_body_param)

qs1_task.set_downstream(qs2_task)