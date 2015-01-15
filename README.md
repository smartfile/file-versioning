versioning_fs
=============
[![Build Status](https://travis-ci.org/smartfile/file-versioning.svg?branch=master)](https://travis-ci.org/smartfile/file-versioning)
[![Coverage Status](https://coveralls.io/repos/travcunn/file-versioning/badge.png)](https://coveralls.io/r/travcunn/file-versioning)
[![Latest Version](https://pypip.in/version/versioning_fs/badge.png)](https://pypi.python.org/pypi/versioning_fs/)
[![License](https://pypip.in/license/versioning_fs/badge.png)](https://pypi.python.org/pypi/versioning_fs/)

Incremental versioning file system for PyFileSystem using rdiff-backup.


### Installation

    pip install versioning_fs

### Requirements

This library depends on 'rdiff-backup'. On Debian based systems:

    sudo apt-get install rdiff-backup


### Usage
TODO: add some examples

     f = self.fs.open(file_name, 'rb', version=3)
