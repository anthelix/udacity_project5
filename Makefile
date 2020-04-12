.PHONY: clean down kill ttyrestart psql .airflow-secret start

help:
	@echo 'Makefile for ETL within AWS											'
	@echo '																		'
	@echo 'Usage: 																'
	@echo ' make config			Stop to enter your credentials
	@echo ' make build			Build images									'
	@echo ' make up			Creates containers and starts service				'
	@echo ' make clean			Stops and removes all docker containers			'
	@echo ' make kill			Kill docker-airflow containers					'
	@echo ' make variable			Setup in eb UI : Admin > variables			'
	@echo ' make down			Stop service and removes containers				'
	@echo ' make stop			Stop environment containers						'
	@echo ' make tty			Open up a shell in the container 				'
	@echo ' make psql			Open up a psql connection with the database		'


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



start: config up .airflow-secret variable

stop:
	$(info Make: Stopping environment containers.)
	@docker-compose stop

clean:	down
	$(info Make: Removing secret files and Docker logs)	
	@rm -f settings/local/secret.yaml
	@docker-compose rm -f
	@rm -rf logs/*

down: stop
	$(info Make: Stopping service and removes containers.)
	@docker-compose down -v

kill:
	$(info Make: Kill docker-airflow containers.)
	@echo "Killing docker-airflow containers"
	docker kill $(shell docker ps -q --filter ancestor=puckel/docker-airflow)

restart: down start

tty:
	docker exec -i -t $(shell docker ps -q --filter ancestor=puckel/docker-airflow) /bin/bash

psql:
	docker exec -i -t $(shell docker ps -q --filter ancestor=postgres:9.6) psql -U airflow
