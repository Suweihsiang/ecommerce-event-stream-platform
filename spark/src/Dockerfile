FROM apache/spark:3.5.6

USER root

ENV SPARK_HOME=/opt/spark

RUN mkdir -p $SPARK_HOME/apps /tmp/.ivy2.5.2 /tmp/.ivy2 /tmp/spark-upload

# 🔥 直接放進 Spark 官方 jars
COPY spark-image/jars/*.jar $SPARK_HOME/jars/

COPY *.py $SPARK_HOME/apps/

RUN chmod +x $SPARK_HOME/apps/*.py

RUN pip install requests