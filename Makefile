clean:
	find . -name *.pyc -delete
	rm __pycache__ -rf

test:
	coverage run tests.py

verify:
	pyflakes versioning_fs
	pep8 versioning_fs

publish:
	python setup.py register
	python setup.py sdist upload
