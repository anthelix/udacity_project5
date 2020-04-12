.PHONY: stop clean down kill up variable tty start restart

help:
	@echo 'Makefile for ETL within AWS											'
	@echo '																		'
	@echo 'Usage: 																'
	@echo ' make build			Build images									'
	@echo ' make up			Creates containers and starts service				'
	@echo ' make clean			Stops and removes all docker containers			'
	@echo ' make kill			Kill docker-airflow containers					'
	@echo ' make variable			Setup in eb UI : Admin > variables			'
	@echo ' make down			Stop service and removes containers				'
	@echo ' make stop			Stop environment containers						'
	@echo ' make tty			Open up a shell in the container 				'
	@echo ' c'est un test1														'
	
.airflow-secret:
	@sleep 10
	@cp settings/local/secret.yaml  settings/.airflow-secret.yaml
	@docker-compose exec webserver bash -c "python3 settings/import-secret.py"
	@rm -f settings/.airflow-secret.yaml
	@touch $@

variable:
	@docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/variables.json
	@echo airflow setup variables

up:
	$(info Make: Starting containers.)
	@docker-compose up --build -d
	@echo airflow running on http://localhost:8080

start: up .airflow-secret variable

stop:
	$(info Make: Stopping environment containers.)
	@docker-compose stop

clean:	down
	$(info Make: Removing secret files and Docker logs)
	@rm -f .airflow-secret
	@rm -f settings/.airflow-secret.yaml
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




# If you see pwd_unknown showing up, this is why. Re-calibrate your system.
PWD ?= pwd_unknown
# PROJECT_NAME defaults to name of the current directory.
# should not to be changed if you follow GitOps operating procedures.
PROJECT_NAME = $(notdir $(PWD))
# Note. If you change this, you also need to update docker-compose.yml.
# only useful in a setting with multiple services/ makefiles.
SERVICE_TARGET := main
# if vars not set specifially: try default to environment, else fixed value.
# strip to ensure spaces are removed in future editorial mistakes.
# tested to work consistently on popular Linux flavors and Mac.
ifeq ($(user),)
# USER retrieved from env, UID from shell.
HOST_USER ?= $(strip $(if $(USER),$(USER),nodummy))
HOST_UID ?= $(strip $(if $(shell id -u),$(shell id -u),4000))
else
# allow override by adding user= and/ or uid=  (lowercase!).
# uid= defaults to 0 if user= set (i.e. root).
HOST_USER = $(user)
HOST_UID = $(strip $(if $(uid),$(uid),0))
endif

THIS_FILE := $(lastword $(MAKEFILE_LIST))
CMD_ARGUMENTS ?= $(cmd)
# export such that its passed to shell functions for Docker to pick up.
export PROJECT_NAME
export HOST_USER
export HOST_UID
test1:
	# here it is useful to add your own customised tests
	docker-compose -p $(PROJECT_NAME)_$(HOST_UID) run --rm $(SERVICE_TARGET) sh -c '\
		echo "I am `whoami`. My uid is `id -u`." && echo "Docker runs!"' \
	&& echo success