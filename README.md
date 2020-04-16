##### Udacity Data Engineering Nanodegree

<img alt="" align="right" width="150" height="150" src = "./image/logo_airflow.png" title = "airflow logo" alt = "airflow logo">  
</br>
</br>
</br>

# Project  : Data Pipelines

About an ETL pipeline for a data lake hosted on S3.

### Table of contents

   - [About The Project](#about-the-project)
   - [Purpose](#purpose)
   - [Getting Started](#getting-started)
        - [Dataset](#dataset)
   - [To Run Localy](#To-run-localy)
        - [Setup Docker](#Setup-Docker)
        - [Setup Your Credentials](#Setup-your-credentials)
        - [Run Scripts](#Run-scripts)    
   - [Worflow](#worflow)
        - [Define a Star Schema](#Define-a-Star-Schema)
        - [Sparkify Analytical](#Sparkify-Analytical)
   - [Web-links](#web-links)


---
## TODO  
to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills  
* pipelines de données 
    * dynamiques
    * construits à partir de tâches réutilisables, 
    * qui puissent être surveillés et 
    * qui permettent un remplissage facile
    * souhaitent effectuer des tests sur leurs ensembles de données après l'exécution des étapes de l'ETL afin de détecter toute divergence dans les ensembles de données.
* créer vos propres opérateurs personnalisés pour effectuer des tâches :
    * telles que la mise en scène des données, 
    * le remplissage de l'entrepôt de données et 
    * l'exécution de contrôles sur les données en tant qu'étape finale.  
* fourni un modèle de projet 
    * qui prend en charge toutes les importations et 
    * fournit quatre opérateurs vides qui doivent être mis en œuvre dans les éléments fonctionnels d'un pipeline de données. 
* Le modèle contient également un ensemble de tâches qui doivent être liées pour obtenir un flux de données cohérent et sensé dans le pipeline.
* Vous disposerez d'une classe d'assistants qui contient toutes les transformations SQL. Ainsi, vous n'aurez pas besoin d'écrire l'ETL vous-même, mais vous devrez l'exécuter avec vos opérateurs personnalisés.
![DAG](image/example-dag.png)

* I then went on to build an example DAG, which would allow me to pull a CSV file from S3, convert to json and then store the result within redshift. 
* \l \c db user \du \z \q \z \d nomtable 

* redshift: changer le chemin par default avant le push de `config.read_file(open(path_cfg + 'dwh.cfg'))` dans `mycluster.py` ligne 12 et 13, `myconn.py`, ligne 13 et 93, `myclusterend.py` ligne


## DONE
* set Docker
    * create docker-compose.yml
---

## About The Project
> A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.  

## Purpose

The purpose of this project is to build an ETL pipeline with Airflow. The source data resides in s3 and need to be processes in Sparkify's data warehouse in Amazon Redshift. The data pipeline should be:
* dynamic
* build from reusable tasks
* can be monitored
* allow backfills
* run tests after the ETL steps have been executed. 

## Getting Started

### Dataset

##### Song Dataset

##### Log Dataset

## To Run localy

* Install Docker
* git clone this project
* In the $WORDIR, run `make config`, setup the `./airflow-secret.yaml` with your credentials then run `make run`. 

#### AWS Redshift
* Todo: Add script to create the redshift database

#### Docker: useful cmd 
* sudo systemctl start docker
* docker images
* docker rmi -f $(docker images -qa)
* docker stop $(docker ps -a -q) 
* docker rm $(docker ps -a -q)
* dud: docker-compose up -d
* docker-compose up -d --build
* docker-compose logs - Displays log output
* docker-compose ps - List containers
* docker-compose down - Stop containers
* If you want to run airflow sub-commands, you can do so like this:
    * docker-compose run --rm webserver airflow list_dags - List dags
    * docker-compose run --rm webserver airflow test [DAG_ID] [TASK_ID] [EXECUTION_DATE] - Test specific task
* If you want to run/test python script, you can do so like this:
    * docker-compose run --rm webserver python /usr/local/airflow/dags/[PYTHON-FILE].py - Test python script
* If you want to use Ad hoc query, make sure you've configured connections: Go to Admin -> Connections and Edit "postgres_default" set this values:
    * localy
        * Host : postgres
        * Schema : sparkify
        * Login : sparkify
        * Password : sparkify
        * Port: 5432

#### Makefile
I provide a Makefile to create and run the Docker container, run Airflow and  create a database Sparkify, user Sparkify
* Run `make` to check the cmd available and see the help menu.



### Setup Docker

### Setup your credentials

### Run scripts

## Worflow