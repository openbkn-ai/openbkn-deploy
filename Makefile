.PHONY: test lint ci test-integration-kafka

test:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*backup.py' -q

lint:
	bash -n deploy.sh
	python3 -c "import ast; from pathlib import Path; [ast.parse(p.read_text()) for p in list(Path('scripts').glob('*backup*.py')) + [Path('scripts/kafka_worker.py')]]"

ci: lint test

# Explicit opt-in: only disposable Kafka containers, reusing the stock Kafka image.
test-integration-kafka:
	docker compose -p openbkn-kafka-backup-test -f tools/kafka-backup/compose.test.yaml up --abort-on-container-exit --exit-code-from verify
