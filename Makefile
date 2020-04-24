.PHONY: clean down kill ttyrestart psql .airflow-secret start redshift

help:
	@echo 'Makefile for ETL within AWS											'
	@echo '																		'
	@echo 'Usage: 																'
	@echo ' make config			Stop to enter your credentials					'
	@echo ' make run			Build containers, setup Airflow					'
	@echo ' make build			Build images									'
	@echo ' make up			Creates containers and starts service				'
	@echo ' make clean			Stops and removes all docker containers			'
	@echo ' make kill			Kill docker-airflow containers					'
	@echo ' make variable			Setup in eb UI : Admin > variables			'
	@echo ' make down			Stop service and removes containers				'
	@echo ' make stop			Stop environment containers						'
	@echo ' make tty			Open up a shell in the container 				'
	@echo ' make psql			Open up a psql connection with the database		'
	@echo ' make start			Buid Docker with config and variables Airflow	'



## MAKE DOCKER

config:
	@cp settings/local/secret_template.yaml  ./airflow-secret.yaml
	@chmod 755 ./airflow-secret.yaml
	$(info Make: >>> Complete the new file "./airflow-secret.yaml" with your credentials, please <<<)	
	
up:
	@cp ./airflow-secret.yaml settings/local/secret.yaml
	@chmod -w settings/local/secret.yaml
	$(info Make: Starting containers)
	@docker-compose up --build -d 
	@echo airflow running on http://localhost:8080

.airflow-secret:
	@rm -f ./airflow-secret.yaml
	@sleep 10	
	@docker-compose exec webserver bash -c "python3 settings/import-secret.py"
	@rm -f settings/local/secret.yaml	

variable:
	@docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/variables.json
	@echo airflow setup variables
	@docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/example_variables.json
	@@echo airflow setup variables

run: up .airflow-secret variable

start: config up .airflow-secret variable

down:
	$(info Make: Stopping service and removes containers.)
	@docker-compose down -v

restart: down start

clean:	down
	$(info Make: Removing secret files and Docker logs)	
	@rm -f settings/local/secret.yaml
	@docker-compose rm -f
	@rm -rf logs/*

kill:
	$(info Make: Kill docker-airflow containers.)
	@echo "Killing docker-airflow containers"
	docker kill $(shell docker ps -q --filter ancestor=puckel/docker-airflow)

tty:
	docker exec -i -t $(shell docker ps -q --filter ancestor=puckel/docker-airflow) /bin/bash

psql:
	docker exec -i -t $(shell docker ps -q --filter ancestor=postgres:9.6) psql -U airflow

## MAKE AWS 

redshift:
	$(info Make: Creating database Redshift and connection.)
	@python3 ./redshift/mycluster.py
	@sleep 400 &
conn:
	@python3 ./redshift/myconn.py 

aws: redshift conn

stop:
	$(info Make: Stopping Redshift.)
	@python3 ./redshift/myclusterend.py &