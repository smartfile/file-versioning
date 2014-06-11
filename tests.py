import os
import random
import shutil
import string
import tempfile
import time
import unittest

from backup import SnapshotInfoError, VersioningFS

PATHS = {'BACKUPS': 'backups',
         'SNAPSHOT_INFO': 'snapshot_info',
         'USER_FILES': 'userfiles',
         'TEMP': 'tmp'}

KB = 1024
MB = pow(1024, 2)


class Paths(dict):
    """
    A dictionary-like object where keys are folders important to the virtual
    file system and values are absolute paths to the folders.

    Parameters:
      base_dir (str): The path to the folder where the file system will store
        its data.
    """
    def __init__(self, base_dir):
        self.__base_dir = base_dir
        self.__paths = {'BACKUPS': 'backups',
                        'SNAPSHOT_INFO': 'snapshot_info',
                        'USER_FILES': 'userfiles',
                        'TEMP': 'tmp'}

    def __getattr__(self, attr):
        path = self.__paths[attr]
        abs_path = os.path.join(self.__base_dir, path)
        return abs_path

    def iterkeys(self):
        return self.__paths.iterkeys()

    def itervalues(self):
        return [self.__getattr__(n) for n in self.__paths.iterkeys()]

    def __repr__(self):
        return repr({n: self.__getattr__(n) for n in self.iterkeys()})


def generate_test_file(fs, path, size, generator=None):
    with fs.open(path, 'wb') as f:
        if generator is None:
            text = '12345678'
        else:
            text = generator().next()
        for _ in range(size/len(text)):
            f.write(text)


def generate_user_files(fs, dir_path, count, size):
    for _ in range(count):
        path = os.path.join(dir_path, random_filename())
        generate_test_file(fs, path, size)


def random_filename(size=20):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))


class BaseTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.paths = Paths(self.temp_dir)
        for folder in self.paths.itervalues():
            os.makedirs(folder)

    def tearDown(self):
        # delete the test dir
        shutil.rmtree(self.temp_dir)


class TestSnapshotAttributes(BaseTest):
    """
    Test meta data manipulation for the files involved in snapshots.
    """
    def setUp(self):
        super(TestSnapshotAttributes, self).setUp()

        self.v = VersioningFS(self.paths.USER_FILES, self.paths.BACKUPS,
                              self.paths.SNAPSHOT_INFO)

        generate_user_files(fs=self.v, dir_path=self.paths.USER_FILES,
                            count=3, size=4*KB)


    def test_snapshot_file_versions(self):
        # make sure no snapshot information exists yet
        self.assert_all_files_have_snapshot_info(should_exist=False)

        # take a snapshot
        self.v.snapshot()
        # make sure each user file is version 0
        self.assert_all_file_versions_equal(0)
        # take another snapshot
        time.sleep(1) # rsync-backup snapshots must be > 1 second apart
        self.v.snapshot()
        # make sure each user file is version 1
        self.assert_all_file_versions_equal(1)

        # add a new file to the user files
        generate_user_files(fs=self.v, dir_path=self.paths.USER_FILES,
                            count=1, size=4*KB)
        # take another snapshot
        time.sleep(1) # rsync-backup snapshots must be > 1 second apart
        self.v.snapshot()
        # not all of the files will be at the same version
        with self.assertRaises(AssertionError):
            self.assert_all_file_versions_equal(2)

        # make sure all files in the user folder have snapshot information
        self.assert_all_files_have_snapshot_info(should_exist=True)

    def test_get_version_of_file_that_does_not_exist(self):
        name = random_filename()
        bad_path = os.path.join(self.paths.TEMP, name)

        with self.assertRaises(SnapshotInfoError):
            self.v.version(bad_path)

    def assert_all_file_versions_equal(self, version):
        for root, dirs, filenames in os.walk(self.paths.USER_FILES):
            for name in filenames:
                path = os.path.join(root, name)

                file_version = self.v.version(path)
                self.assertEqual(file_version, version)

    def assert_all_files_have_snapshot_info(self, should_exist=True):
        for root, dirs, filenames in os.walk(self.paths.USER_FILES):
            for name in filenames:
                path = os.path.join(root, name)
                snapshot_info_exists = self.v.has_snapshot(path)

                self.assertEqual(snapshot_info_exists, should_exist)


class TestFileVersions(BaseTest):
    """
    Test file versions.
    """
    def setUp(self):
        super(TestFileVersions, self).setUp()

        self.v = VersioningFS(self.paths.USER_FILES, self.paths.BACKUPS,
                              self.paths.SNAPSHOT_INFO)

    def test_open_single_file_no_versioning(self):
        file_name = random_filename()

        repeat_text = 'smartfile_versioning_rocks_\n'
        def file_contents():
            while True:
                yield repeat_text

        abs_path = os.path.join(self.paths.USER_FILES, file_name)
        generate_test_file(fs=self.v, path=abs_path, size=5*KB,
                           generator=file_contents)

        with self.v.open(file_name, 'rb') as f:
            for line in f:
                self.assertEqual(line, file_contents().next())

"""
class TestVersioningFS(BaseTest):
    def setUp(self):
        super(TestVersioningFS, self).setUp()

        generate_user_files(count=3, size=1*MB)
        self.v = VersioningFS(PATHS['USER_FILES'], PATHS['BACKUPS'],
                              PATHS['SNAPSHOT_INFO'])

    def test_something(self):
        self.assertTrue(True)

    def tearDown(self):
        super(TestVersioningFS, self).tearDown()
        pass
"""


if __name__ == "__main__":
    unittest.main()
