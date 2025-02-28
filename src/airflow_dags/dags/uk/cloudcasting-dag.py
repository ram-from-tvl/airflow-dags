"""Dag to run the cloudcasting app."""

import datetime as dt
import os

from airflow import DAG
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from utils.slack import slack_message_callback

env = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=1),
    "max_active_runs": 10,
    "concurrency": 10,
    "max_active_tasks": 10,
}
default_task_args = {
    "cluster": f"Nowcasting-{env}",
    "task_definition": "satellite-consumer",
    "launch_type": "FARGATE",
    "task_concurrency": 10,
    "network_configuration": {
        "awsvpcConfiguration": {
            "subnets": [os.getenv("ECS_SUBNET")],
            "securityGroups": [os.getenv("ECS_SECURITY_GROUP")],
            "assignPublicIp": "ENABLED",
        },
    },
    "awslogs_group": "/aws/ecs/forecast/cloudcasting",
    "awslogs_stream_prefix": "streaming/cloudcasting",
    "awslogs_region": "eu-west-1",
}

@dag(
    "uk-cloudcasting",
    description=__doc__,
    schedule_interval="20,50 * * * *",
    start_date=dt.datetime(2025, 2, 1, tzinfo=dt.UTC),
    catchup=False,
    default_args=default_args,
)
def cloudcasting_dag() -> DAG:
    """Dag to run the cloudcasting app."""
    EcsRunTaskOperator(
        task_id="uk-cloudcasting",
        task_definition="cloudcasting",
        on_failure_callback=slack_message_callback(
            "⚠️ The task {{ ti.task_id }} failed,"
            " but its ok. The cloudcasting is currently not critical. "
            "No out of hours support is required.",
        ),
        **default_task_args,
    )

cloudcasting_dag()

