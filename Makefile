clean:
	find . -name *.pyc -delete

test:
	coverage run tests.py
