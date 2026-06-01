from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="events_medallion_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 27),
    schedule="0 * * * *",  # 每小時跑一次
    catchup=False,
    tags=["spark", "silver", "gold", "iceberg", "minio"],
) as dag:

    bronze_to_silver = KubernetesPodOperator(
        task_id="bronze_to_silver_events",
        name="bronze-to-silver-events",
        namespace="spark",
        image="kafka-to-minio:latest",
        image_pull_policy="IfNotPresent",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc:443",
            "--deploy-mode", "cluster",
            "--name", "bronze-to-silver",
            "--conf", "spark.kubernetes.namespace=spark",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
            "--conf", "spark.executor.instances=1",
            "--conf", "spark.executor.memory=1g",
            "--conf", "spark.executor.memoryOverhead=512m",
            "--conf", "spark.executor.cores=1",
            "--conf", "spark.driver.memory=1g",
            "--conf", "spark.driver.memoryOverhead=512m",
            "--conf", "spark.kubernetes.container.image=kafka-to-minio:latest",
            "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
            "--conf", "spark.kubernetes.driver.deleteOnTermination=true",
            "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "local:///opt/spark/apps/bronze-to-silver.py",
        ],
        get_logs=True,
        log_events_on_failure=True,
        on_finish_action="delete_pod",
    )

    silver_to_gold = KubernetesPodOperator(
        task_id="silver_to_gold_event_type_count",
        name="silver-to-gold-event-type-count",
        namespace="spark",
        image="kafka-to-minio:latest",
        image_pull_policy="IfNotPresent",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc:443",
            "--deploy-mode", "cluster",
            "--name", "silver-to-gold",
            "--conf", "spark.kubernetes.namespace=spark",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
            "--conf", "spark.executor.instances=1",
            "--conf", "spark.executor.memory=1g",
            "--conf", "spark.executor.memoryOverhead=512m",
            "--conf", "spark.executor.cores=1",
            "--conf", "spark.driver.memory=1g",
            "--conf", "spark.driver.memoryOverhead=512m",
            "--conf", "spark.kubernetes.container.image=kafka-to-minio:latest",
            "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
            "--conf", "spark.kubernetes.driver.deleteOnTermination=true",
            "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "local:///opt/spark/apps/silver-to-gold.py",
        ],
        get_logs=True,
        log_events_on_failure=True,
        on_finish_action="delete_pod",
    )

    check_data_quality = KubernetesPodOperator(
        task_id="check-data-quality",
        name="check-data-quality",
        namespace="spark",
        image="kafka-to-minio:latest",
        image_pull_policy="IfNotPresent",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc:443",
            "--deploy-mode", "cluster",
            "--name", "check-data-quality",
            "--conf", "spark.kubernetes.namespace=spark",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
            "--conf", "spark.executor.instances=1",
            "--conf", "spark.executor.memory=1g",
            "--conf", "spark.executor.memoryOverhead=512m",
            "--conf", "spark.executor.cores=1",
            "--conf", "spark.driver.memory=1g",
            "--conf", "spark.driver.memoryOverhead=512m",
            "--conf", "spark.kubernetes.driver.secretKeyRef.TELEGRAM_BOT_TOKEN=telegram-secret:TELEGRAM_BOT_TOKEN",
            "--conf", "spark.kubernetes.driver.secretKeyRef.TELEGRAM_CHAT_ID=telegram-secret:TELEGRAM_CHAT_ID",
            "--conf", "spark.kubernetes.container.image=kafka-to-minio:latest",
            "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
            "--conf", "spark.kubernetes.driver.deleteOnTermination=true",
            "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "local:///opt/spark/apps/check-data-quality.py",
        ],
        get_logs=True,
        log_events_on_failure=True,
        on_finish_action="delete_pod",
        env_from = [
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(
                    name="telegram-secret"
                )
            )
        ],
    )


bronze_to_silver >> silver_to_gold >> check_data_quality