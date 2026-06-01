FROM python:3.11-slim

WORKDIR app
COPY . app

RUN pip install kafka-python

ENTRYPOINT ["python3","app/job.py"]