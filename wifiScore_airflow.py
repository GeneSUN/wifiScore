from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 
import airflow.settings
from airflow.models import DagModel
import time

from datetime import datetime, timedelta, date 
from textwrap import dedent
import sys

default_args = { 
    'owner': 'ZheSun', 
    'depends_on_past': False, 
} 

with DAG(
    dag_id="wifiScore_extenderScore",
    default_args=default_args,
    description="get wifiScore and extender Score",
    schedule_interval="00 21 * * *",
    start_date=days_ago(1),
    tags=["wifiScore","extenderScore"],
    catchup=False,
    max_active_runs=1,
) as dag:
    task_wifiScore_v3 = BashOperator( 
                        task_id="ClasswifiScore_v3", 
                        bash_command = f"/usr/apps/vmas/script/ZS/wifiScore/ClasswifiScore_v3.sh ", 
                        env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
                        dag=dag, 
                            ) 

    task_extender_list = BashOperator( 
                    task_id="extender_list", 
                    bash_command = f"/usr/apps/vmas/script/ZS/wifiScore/extender_list.sh ", 
                    env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
                    dag=dag, 
                        ) 
    task_prepare_router_test_data = BashOperator( 
                task_id="prepare_router_test_data", 
                bash_command = f"/usr/apps/vmas/script/ZS/wifiScore/prepare_router_test_data.sh ", 
                env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
                dag=dag, 
                    ) 
    task_extenderScore = BashOperator( 
                task_id="extenderScore", 
                bash_command = f"/usr/apps/vmas/script/ZS/wifiScore/extenderScore.sh ", 
                env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
                dag=dag, 
                    ) 
    task_wifiScore_v3 >> [ task_prepare_router_test_data ,task_extender_list ] 
    task_extender_list >> task_extenderScore
    task_prepare_router_test_data >> task_extenderScore