#!/usr/bin/env python
from setuptools import setup

name = 'versioning_fs'

try:
   import pypandoc
   long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
   long_description = ''

setup (
    name = name,
    version = '0.1.5',
    description = """Incremental versioning file system for PyFileSystem
                     using rdiff-backup.""",
    long_description = long_description,
    author = 'Travis Cunningham',
    author_email = 'travcunn@umail.iu.edu',
    maintainer = 'Travis Cunningham',
    maintainer_email = 'travcunn@umail.iu.edu',
    url = 'http://github.com/travcunn/file-versioning',
    license = 'MIT',
    packages = ['versioning_fs'],
    package_dir = {'versioning_fs' : 'versioning_fs'},
    install_requires = ['fs'],
)
