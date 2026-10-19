.PHONY: test lint ci

test:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*backup.py' -q

lint:
	bash -n deploy.sh
	python3 -c "import ast; from pathlib import Path; [ast.parse(p.read_text()) for p in Path('scripts').glob('*backup*.py')]"

ci: lint test
