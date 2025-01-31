.PHONY: test-processor test-coordinator test-aggregator test-all

test-processor:
	.github/scripts/test_workflow.sh processor

test-coordinator:
	.github/scripts/test_workflow.sh coordinator

test-aggregator:
	.github/scripts/test_workflow.sh aggregator

test-all: test-processor test-coordinator test-aggregator

setup:
	docker-compose up -d
	docker-compose exec test-env apt-get update
	docker-compose exec test-env apt-get install -y python3-pip
	docker-compose exec test-env pip3 install pandas pyarrow tqdm selenium

clean:
	rm -rf /tmp/artifacts/*
	docker-compose down 