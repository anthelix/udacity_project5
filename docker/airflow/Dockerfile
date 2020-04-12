FROM puckel/docker-airflow

USER root

RUN set -xe \
  && pip install papermill flake8 boto3 \
	  awscli \
	  sql_magic \
    apache-airflow[postgres,s3] \
  && python3 -m ipykernel install

RUN pip install --user psycopg2-binary
ENV AIRFLOW_HOME=/usr/local/airflow
COPY settings/airflow.cfg /usr/local/airflow/airflow.cfg


USER airflow
RUN mkdir -p ~/.aws

