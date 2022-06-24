from genericpath import exists
import os
import logging

from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# custom utils
from utils.job_config import JobConfig

JOB_ARGS = JobConfig.get_config()
DEFAULTS = JOB_ARGS["default_args"]
ENV = JOB_ARGS["env_name"]
TEAM_NAME = JOB_ARGS["team_name"]
SF_CONN_ID = JOB_ARGS["snowflake_conn_id"]
SF_ROLE = JOB_ARGS["snowflake"]["role"]
SF_WAREHOUSE = JOB_ARGS["snowflake"]["warehouse"]
SF_DATABASE = JOB_ARGS["snowflake"]["database"]
stage_sql_path = "adlogs/log_process"



# create DAG
dag_id = 'stage_dag_cont'

@dag(
dag_id=dag_id,
start_date=datetime(2019, 1, 1),
max_active_runs=1,
schedule_interval=JOB_ARGS['schedule_interval'],
default_args=DEFAULTS,
default_view="tree", # This defines the default view for this DAG in the Airflow UI
catchup=False,
tags=["Airflow2.0"] # If set, this tag is shown in the DAG view of the Airflow UI
)

def stage_second_dag():

    stage_finish = DummyOperator(task_id="staging_cont_finish")

    dag_sensor = ExternalTaskSensor(
        task_id = "dag_sensor",
        external_dag_id= "stage_adlogs"
    )

    for table in JOB_ARGS["tables"]:
        if table in JOB_ARGS["ipn_table"]:
            ipn_job = SnowflakeOperator(
                task_id = "ipn_{}".format(table),
                snowflake_conn_id=SF_CONN_ID,
                warehouse=SF_WAREHOUSE,
                database=SF_DATABASE,
                sql=f'sql/{stage_sql_path}/ipn_blacklist.sql',
                params={
                    "env": ENV,
                    "team_name": TEAM_NAME,
                    "table": table
                },
                autocommit=True,
                trigger_rule='all_done'
            )

        if table in JOB_ARGS["smart_table"]:
            smart_job = SnowflakeOperator(
                task_id = "smart_{}".format(table),
                snowflake_conn_id=SF_CONN_ID,
                warehouse=SF_WAREHOUSE,
                database=SF_DATABASE,
                sql=f'sql/{stage_sql_path}/smart_clip_patch.sql',
                params={
                    "env": ENV,
                    "team_name": TEAM_NAME,
                    "table": table
                },
                autocommit=True,
                trigger_rule='all_done'
            )

        if table in JOB_ARGS["set_table"]:
            set_job = SnowflakeOperator(
                task_id = "set_{}".format(table),
                snowflake_conn_id=SF_CONN_ID,
                warehouse=SF_WAREHOUSE,
                database=SF_DATABASE,
                sql=f'sql/{stage_sql_path}/set_interaction_smartclip_placement.sql',
                params={
                    "env": ENV,
                    "team_name": TEAM_NAME,
                    "table": table
                },
                autocommit=True,
                trigger_rule='all_done'
            )
        dag_sensor >> ipn_job >> smart_job >> stage_finish
    dag_sensor >> set_job >> stage_finish
stage_second_dag = stage_second_dag()