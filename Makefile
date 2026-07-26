.PHONY: build test unit integration experiment spark k8s-test

build:
	mvn -DskipTests package

test:
	mvn test

unit:
	mvn -Dgroups='!integration' test

integration:
	./scripts/run-integration-tests.sh

experiment:
	./scripts/run-experiment.sh

spark:
	docker compose --profile spark run --rm spark-discovery

k8s-test:
	./scripts/test-kubernetes.sh
