# --------------------------------------------------------------- venv

.PHONY: init # initialize venv
init:
	# get requirements.in
	pip install pip --upgrade
	pip install pipreqs
	rm -rf requirements.txt requirements.in
	pipreqs . --mode no-pin --encoding utf-8
	mv requirements.txt requirements.in

	# get requirements.txt
	pip install pip-tools
	pip-compile requirements.in -o requirements.txt -vvv
	
	# install everything in venv
	rm -rf .venv
	python3 -m venv .venv
	@bash -c "source .venv/bin/activate && pip install -r requirements.txt"

.PHONY: lock # freeze pip and lock reqs
lock:
	@bash -c "source .venv/bin/activate && pip freeze > requirements.in"
	pip-compile requirements.in -o requirements.txt

# --------------------------------------------------------------- nohup

.PHONY: monitor # create nohup with restart on failure
monitor:
	@if [ "$(filepath)" = "" ]; then echo "missing 'path' argument"; exit 1; fi
	@bash -c '\
		monitor() { \
			while true; do \
				if ! ps -p $$(cat "monitor-process.pid" 2>/dev/null) > /dev/null 2>&1; then \
					echo "$$(date): process not running or died, (re)starting..." >> monitor.log; \
					nohup ./.venv/bin/python3 "$(filepath)" > "monitor-process.log" 2>&1 & \
					echo $$! > "monitor-process.pid"; \
					echo "$$(date): started process with PID $$(cat monitor-process.pid)" >> monitor.log; \
				fi; \
				sleep 5; \
			done; \
		}; \
		monitor >> "monitor.log" 2>&1 & \
		echo $$! > "monitor.pid"; \
		echo "$$(date): monitor started" >> "monitor.log"; \
	'

.PHONY: monitor-tail # tail log of nohup process
monitor-tail:
	while true; do clear; tail -n 100 monitor-process.log; sleep 0.1; done
	# watch -n 0.1 "tail -n 100 monitor-process.log"

.PHONY: monitor-kill # kill nohup process
monitor-kill:
	-kill -9 $$(cat monitor.pid)
	rm -rf monitor.pid
	rm -rf monitor.log
	-kill -9 $$(cat monitor-process.pid)
	rm -rf monitor-process.pid
	rm -rf monitor-process.log

# --------------------------------------------------------------- docker

.PHONY: docker-install # run docker container
docker-install:
	@echo "to exec into docker container, run: 'docker exec -it main /bin/bash'"
	docker-compose up --detach

.PHONY: docker-clean # wipe everything in docker
docker-clean:
	docker-compose down

	-docker stop $$(docker ps -a -q)
	-docker rm $$(docker ps -a -q)
	-docker rmi $$(docker images -q)
	yes | docker container prune
	yes | docker image prune
	yes | docker volume prune
	yes | docker network prune
	yes | docker system prune
	
	docker ps --all
	docker images
	docker system df
	docker volume ls
	docker network ls

# --------------------------------------------------------------- utils

.PHONY: fmt # format codebase
fmt:
	pip install isort
	pip install ruff
	pip install autoflake

	isort .
	autoflake --remove-all-unused-imports --recursive --in-place .
	ruff format --config line-length=500 .

.PHONY: sec # check for vulns
sec:
	pip install bandit
	pip install safety
	
	bandit -r .
	safety check --full-report

.PHONY: up # pull and push changes
up:
	git pull
	git add .
	if [ -z "$(msg)" ]; then git commit -m "up"; else git commit -m "$(msg)"; fi
	git push

.PHONY: help # generate help message
help:
	@echo "Usage: make [target]\n"
	@grep '^.PHONY: .* #' Makefile | sed 's/\.PHONY: \(.*\) # \(.*\)/\1	\2/' | expand -t20
