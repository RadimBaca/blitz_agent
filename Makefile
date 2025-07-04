.PHONY: test

test:
	python -c "import sys, os; sys.path.insert(0, os.path.abspath('.')); import pytest; raise SystemExit(pytest.main(['tests/test_result_DAO.py']))"

run:
	python app.py