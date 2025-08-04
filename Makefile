.PHONY: test lint lint-fix

test:
	python -c "import sys, os; sys.path.insert(0, os.path.abspath('.')); import pytest; raise SystemExit(pytest.main(['tests/test_result_DAO.py']))"

# Run pylint on source code
lint:
	python3 -m pylint src/ tests/

# Run pylint with score only (for CI/CD)
lint-score:
	python3 -m pylint src/ tests/ --score=yes --reports=no

# Auto-fix some pylint issues using autopep8
lint-fix:
	autopep8 --in-place --recursive src/ tests/

run:
	python app.py