from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

local_tz = pendulum.timezone("Asia/Taipei")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cleanup_iceberg",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 5, 9, 3, 0, tz=local_tz),
    schedule="0 */6 * * *",  # 每天 UTC 03:00 跑一次
    catchup=False,
    tags=["spark", "iceberg", "cleanup"],
) as dag:

    cleanup_iceberg = KubernetesPodOperator(
        task_id="cleanup_iceberg",
        name="cleanup-iceberg",
        namespace="spark",
        image="kafka-to-minio:latest",
        image_pull_policy="IfNotPresent",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc:443",
            "--deploy-mode", "cluster",
            "--name", "cleanup-iceberg",

            "--conf", "spark.kubernetes.namespace=spark",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",

            "--conf", "spark.executor.instances=1",
            "--conf", "spark.executor.memory=1g",
            "--conf", "spark.executor.memoryOverhead=512m",
            "--conf", "spark.executor.cores=1",

            "--conf", "spark.driver.memory=1g",
            "--conf", "spark.driver.memoryOverhead=512m",

            "--conf", "spark.sql.shuffle.partitions=4",

            "--conf", "spark.kubernetes.container.image=kafka-to-minio:latest",
            "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
            "--conf", "spark.kubernetes.driver.deleteOnTermination=true",

            "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

            "local:///opt/spark/apps/cleanup-iceberg.py",
        ],
        get_logs=True,
        log_events_on_failure=True,
        on_finish_action="delete_pod",
    )