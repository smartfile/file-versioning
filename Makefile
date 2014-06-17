clean:
	find . -name *.pyc -delete

test:
	python tests.py

verify:
	pyflakes versioning_fs
	pep8 versioning_fs
