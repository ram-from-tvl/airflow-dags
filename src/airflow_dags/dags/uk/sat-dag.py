"""Dag to download and process satellite data from EUMETSAT.

Consists of two tasks made from the same ECS operator,
one for RSS data and one for Odegree data.
The 0degree data task only runs if the RSS data task fails.
"""

import datetime as dt
import os

from airflow import DAG
from airflow.decorators import dag
from airflow.utils.trigger_rule import TriggerRule
from utils.slack import slack_message_callback

from airflow_dags.plugins.operators.ecs_run_task_operator import ECSOperatorGen

env = os.getenv("ENVIRONMENT", "development")

default_dag_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
    "max_active_runs": 10,
    "concurrency": 10,
    "max_active_tasks": 10,
    "execution_timeout": dt.timedelta(minutes=30),
}

sat_consumer = ECSOperatorGen(
    name="satellite-consumer"
    container_image="ghcr.io/openclimatefix/satellite-consumer",
    container_tag="0.0.5",
    container_env={
        "LOGLEVEL": "DEBUG",
        "SATCONS_COMMAND": "consume",
        "SATCONS_VALIDATE": "true",
        "SATCONS_RESCALE": "true",
    },
    container_secret_env={
        "development/data/satellite-consumer": [
            "EUMETSAT_CONSUMER_KEY", "EUMETSAT_CONSUMER_SECRET",
        ],
    },
    domain="uk",
)


@dag(
    dag_id="uk-satellite-consumer",
    description=__doc__,
    schedule_interval="*/5 * * * *",
    start_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
    catchup=False,
    default_args=default_dag_args,
)
def sat_consumer_dag() -> DAG:
    """Dag to download and process satellite data from EUMETSAT."""
    sat_consumer_rss = EcsRunTaskOperator(
        task_id="satellite-consumer-rss",
        overrides={"containerOverrides": [{
            "name": "satellite-consumer-consumer",
            "environment": [
                {"name": "SATCONS_SATELLITE", "value": "rss"},
                {"name": "SATCONS_WORKDIR", "value": f"s3://nowcasting-sat-{env}/testdata"},
            ],
        }]},
        **default_task_args,
    )

    sat_consumer_odegree = EcsRunTaskOperator(
        task_id="satellite-consumer-odegree",
        trigger_rule=TriggerRule.ALL_FAILED,
        overrides={"containerOverrides": [{
            "name": "satellite-consumer-consumer",
            "environment": [
                {"name": "SATCONS_SATELLITE", "value": "odegree"},
                {"name": "SATCONS_WORKDIR", "value": f"s3://nowcasting-sat-{env}/testdata"},
            ],
        }]},
        on_failure_callback=slack_message_callback(
            "⚠️ The task {{ ti.task_id }} failed to collect odegree satellite data. "
            "The forecast will automatically move over to PVNET-ECMWF "
            "which doesn't need satellite data. "
            "Forecast quality may be impacted, but no out-of-hours support is required. "
            "Please log in an incident log. ",
        ),
        **default_task_args,
    )

    sat_consumer_rss >> sat_consumer_odegree

sat_consumer_dag()

