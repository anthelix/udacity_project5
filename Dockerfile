FROM puckel/docker-airflow

USER root

RUN set -xe \
  && pip install papermill flake8 boto3 \
	  awscli \
	  sql_magic \
    apache-airflow[postgres,s3] \
  && python3 -m ipykernel install

USER airflow

RUN mkdir -p ~/.aws