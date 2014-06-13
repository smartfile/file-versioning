import os
import random
import shutil
import string
import tempfile
import time
import unittest

from versioning_fs import VersioningFS, VersionManager


PATHS = {'BACKUPS': 'backups',
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
        self.__paths = PATHS

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


class DictVersionManager(VersionManager):
    def __init__(self):
        super(DictVersionManager, self).__init__()
        self.__files = {}

    def has_snapshot(self, path):
        if self.__files.get(path) is not None:
            return True
        return False

    def version(self, path):
        version = self.__files.get(path)
        return version

    def set_version(self, path, version):
        self.__files[path] = version

    def remove(self, path):
        if path in self.__files:
            self.__files.pop(path, None)


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

        self.v = VersioningFS(DictVersionManager(), self.paths.USER_FILES,
                              self.paths.BACKUPS, self.paths.TEMP)

    def test_snapshot_file_versions(self):
        # make sure no snapshot information exists yet
        self.assert_all_files_have_snapshot_info(should_exist=False)

        repeat_text = 'smartfile_versioning_rocks_\n'
        def file_contents():
            while True:
                yield repeat_text

        # generate file 1
        file_name = random_filename()
        abs_path = os.path.join(self.paths.USER_FILES, file_name)
        generate_test_file(fs=self.v, path=abs_path, size=5*KB,
                           generator=file_contents)

        # make sure each user file is version 1
        self.assert_all_file_versions_equal(1)

        # generate file 2
        file_name = random_filename()
        abs_path = os.path.join(self.paths.USER_FILES, file_name)
        generate_test_file(fs=self.v, path=abs_path, size=5*KB,
                           generator=file_contents)

        # make sure each user file is version 1
        self.assert_all_file_versions_equal(1)

        # take another snapshot of file 2
        time.sleep(1) # rsync-backup snapshots must be > 1 second apart
        self.v.snapshot(file_name)

        # check that the updated file is at version 2
        self.assertEqual(self.v._v_manager.version(file_name), 2)

        # not all of the files will be at the same version
        with self.assertRaises(AssertionError):
            self.assert_all_file_versions_equal(1)

        # check that only one file was updated to version 1
        self.v.remove(file_name)
        self.assert_all_file_versions_equal(1)

        # make sure all files in the user folder have snapshot information
        self.assert_all_files_have_snapshot_info(should_exist=True)

    def assert_all_file_versions_equal(self, version):
        for root, dirs, filenames in os.walk(self.paths.USER_FILES):
            for name in filenames:
                file_version = self.v._v_manager.version(name)
                self.assertEqual(file_version, version)

    def assert_all_files_have_snapshot_info(self, should_exist=True):
        for root, dirs, filenames in os.walk(self.paths.USER_FILES):
            for name in filenames:
                snapshot_info_exists = self.v._v_manager.has_snapshot(name)
                self.assertEqual(snapshot_info_exists, should_exist)


class TestFileVersions(BaseTest):
    """
    Test file versions.
    """
    def setUp(self):
        super(TestFileVersions, self).setUp()

        self.v = VersioningFS(DictVersionManager(), self.paths.USER_FILES,
                              self.paths.BACKUPS, self.paths.TEMP)

    def test_single_file_updating(self):
        file_name = random_filename()

        f = self.v.open(file_name, 'wb')
        f.write('smartfile_versioning_rocks\n')
        f.close()

        # check that version 1 was created
        self.assertEqual(self.v._v_manager.version(file_name), 1)

        f = self.v.open(file_name, 'rb')
        self.assertEqual(f.read(), 'smartfile_versioning_rocks\n')
        f.close()

        time.sleep(1) # sleep in between snapshots

        # make some changes to the file and check for version increment
        f = self.v.open(file_name, 'wb')
        f.write("hello world!")
        f.close()
        self.assertEqual(self.v._v_manager.version(file_name), 2)

        # check the contents when we open the file
        f = self.v.open(file_name, 'rb')
        self.assertEqual(f.read(), "hello world!")
        f.close()
        # make sure the version has not been updated
        self.assertEqual(self.v._v_manager.version(file_name), 2)

    def test_open_old_version(self):
        file_name = random_filename()

        f = self.v.open(file_name, 'wb')
        f.write("smartfile")
        f.close()

        time.sleep(1)

        f = self.v.open(file_name, 'wb')
        f.write("smartfile versioning")
        f.close()

        time.sleep(1)

        f = self.v.open(file_name, 'wb')
        f.write("smartfile versioning rocks")
        f.close()

        # now try opening previous versions of the file and check content
        f = self.v.open(file_name, 'rb', version=1)
        self.assertEqual(f.read(), "smartfile")
        f.close()

        f = self.v.open(file_name, 'rb', version=2)
        self.assertEqual(f.read(), "smartfile versioning")
        f.close()

        f = self.v.open(file_name, 'rb', version=3)
        self.assertEqual(f.read(), "smartfile versioning rocks")
        f.close()

        # the file version has not changed since we only read the version
        self.assertEqual(self.v._v_manager.version(file_name), 3)


if __name__ == "__main__":
    unittest.main()
