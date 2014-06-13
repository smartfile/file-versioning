#!/usr/bin/env python
from setuptools import setup

setup (
    name = 'versioning_fs',
    version = '0.0.1',
    description = """A file system abstraction that stores previous revisions
                  of files, with the ability to restore any version.""",
    author = 'Travis Cunningham',
    author_email = 'travcunn@umail.iu.edu',
    packages = ['versioning_fs'],
    package_dir = {'versioning_fs' : 'versioning_fs'},
    install_requires = ['fs'],
)
