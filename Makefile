.PHONY: test lint ci

test:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_backup.py -q

lint:
	bash -n deploy.sh
	python3 -c "import ast; from pathlib import Path; [ast.parse(Path(p).read_text()) for p in ('scripts/backup.py', 'scripts/test_backup.py')]"

ci: lint test
