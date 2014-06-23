import os
import random
import shutil
import string
import tempfile
import unittest

from fs.errors import ResourceNotFoundError
from fs.path import relpath
from fs.tempfs import TempFS
from fs.tests import FSTestCases
from fs.tests import ThreadingTestCases

from versioning_fs import VersioningFS


KB = 1024
MB = pow(1024, 2)


def generate_file(fs, path, size, generator=None):
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
        generate_file(fs, path, size)


def random_filename(size=20):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))


class BaseTest(unittest.TestCase):
    def setUp(self):
        self.__tempfs = TempFS()
        self.__scratch_dir = tempfile.mkdtemp()

        self.fs = VersioningFS(self.__tempfs, backup_dir='abcdefg',
                               tmp=self.__scratch_dir, testing={'time': 1})

    def tearDown(self):
        self.fs.close()
        shutil.rmtree(self.__scratch_dir)


class TestVersioningFS(FSTestCases, ThreadingTestCases, BaseTest):
    pass


class TestSnapshotAttributes(BaseTest):
    """Test meta data manipulation for the files involved in snapshots."""
    def test_snapshot_file_versions(self):
        # make sure no snapshot information exists yet
        self.assert_all_files_have_snapshot_info(should_exist=False)

        repeat_text = 'smartfile_versioning_rocks_\n'
        def file_contents():
            while True:
                yield repeat_text

        # generate file 1
        file_name = random_filename()
        generate_file(fs=self.fs, path=file_name, size=5*KB,
                      generator=file_contents)

        # make sure each user file is version 1
        self.assert_all_file_versions_equal(1)

        # generate file 2
        file_name = random_filename()
        generate_file(fs=self.fs, path=file_name, size=5*KB,
                      generator=file_contents)

        # make sure each user file is version 1
        self.assert_all_file_versions_equal(1)

        with self.fs.open(file_name, 'wb') as f:
            f.write('hello world')

        # check that the updated file is at version 2
        self.assertEqual(self.fs.version(file_name), 2)

        # not all of the files will be at the same version
        with self.assertRaises(AssertionError):
            self.assert_all_file_versions_equal(1)

        # check that only one file was updated to version 1
        self.fs.remove(file_name)
        self.assert_all_file_versions_equal(1)

        # make sure all files in the user folder have snapshot information
        self.assert_all_files_have_snapshot_info(should_exist=True)

    def assert_all_file_versions_equal(self, version):
        for path in self.fs.walkfiles('/'):
            if not 'abcdefg' in path and 'tmp' not in path:
                path = relpath(path)
                file_version = self.fs.version(path)
                self.assertEqual(file_version, version)

    def assert_all_files_have_snapshot_info(self, should_exist=True):
        for path in self.fs.walkfiles('/'):
            if not 'abcdefg' in path and 'tmp' not in path:
                path = relpath(path)
                snapshot_info_exists = self.fs.has_snapshot(path)
                self.assertEqual(snapshot_info_exists, should_exist)


class TestFileVersions(BaseTest):
    """Test file versions."""
    def test_single_file_updating(self):
        file_name = random_filename()

        f = self.fs.open(file_name, 'wb')
        f.write('smartfile_versioning_rocks\n')
        f.close()

        # check that version 1 was created
        self.assertEqual(self.fs.version(file_name), 1)

        f = self.fs.open(file_name, 'rb')
        self.assertEqual(f.read(), 'smartfile_versioning_rocks\n')
        f.close()

        # make some changes to the file and check for version increment
        f = self.fs.open(file_name, 'wb')
        f.writelines("hello world!\nhello world!")
        f.close()
        self.assertEqual(self.fs.version(file_name), 2)

        # check the contents when we open the file
        f = self.fs.open(file_name, 'rb')
        self.assertEqual(f.readlines(), ["hello world!\n", "hello world!"])
        f.close()
        # make sure the version has not been updated
        self.assertEqual(self.fs.version(file_name), 2)

    def test_open_old_version(self):
        file_name = random_filename()

        f = self.fs.open(file_name, 'wb')
        f.write("smartfile")
        f.close()

        f = self.fs.open(file_name, 'wb')
        f.write("smartfile versioning")
        f.close()

        f = self.fs.open(file_name, 'wb')
        f.write("smartfile versioning rocks")
        f.close()

        # now try opening previous versions of the file and check content
        f = self.fs.open(file_name, 'rb', version=1)
        self.assertEqual(f.read(), "smartfile")
        f.close()

        f = self.fs.open(file_name, 'rb', version=2)
        self.assertEqual(f.read(), "smartfile versioning")
        f.close()

        f = self.fs.open(file_name, 'rb', version=3)
        self.assertEqual(f.read(), "smartfile versioning rocks")
        f.close()

        # the file version has not changed since we only read the version
        self.assertEqual(self.fs.version(file_name), 3)

    def test_bad_version(self):
        repeat_text = 'smartfile_versioning_rocks_\n'
        def file_contents():
            while True:
                yield repeat_text

        # generate file 1
        file_name = random_filename()
        generate_file(fs=self.fs, path=file_name, size=5*KB,
                      generator=file_contents)

        with self.assertRaises(ResourceNotFoundError):
            self.fs.open(file_name, 'rb', version=0)

        with self.assertRaises(ResourceNotFoundError):
            self.fs.open(file_name, 'rb', version=2)


class TestRdiffBackupLimitations(unittest.TestCase):
    """Rdiff backup cannot make two snapshots within 1 second.
       This test checks that the filewrapper sleeps for 1 second before
       trying to make a snapshot.
    """
    def setUp(self):
        self.__tempfs = TempFS()
        self.__scratch_dir = tempfile.mkdtemp()

        self.fs = VersioningFS(self.__tempfs, backup_dir='abcdefg',
                               tmp=self.__scratch_dir)

    def test_quick_file_changes(self):
        # test two file edits within 1 second
        file_name = random_filename()

        f = self.fs.open(file_name, 'wb')
        f.write("smartfile")
        f.close()

        f = self.fs.open(file_name, 'wb')
        f.write("smartfile versioning")
        f.close()

    def tearDown(self):
        self.fs.close()
        shutil.rmtree(self.__scratch_dir)


class TestFileOperations(BaseTest):
    """Test fs.move, fs.movedir, fs.remove, and fs.removedir"""
    def test_move_single_file(self):
        """Move a single file, which should also move its backups."""
        # have 2 versions of a file we create
        file_name = random_filename()

        f = self.fs.open(file_name, 'wb')
        f.write("smartfile")
        f.close()

        f = self.fs.open(file_name, 'wb')
        f.write("smartfile versioning")
        f.close()

        # move the file somewhere else
        new_filename = random_filename()
        self.fs.move(file_name, new_filename)

        # check if versioning is still available
        f = self.fs.open(new_filename, 'rb', version=2)
        self.assertEqual(f.read(), "smartfile versioning")
        f.close()

    def test_move_single_directory(self):
        """Move a directory, which should also move the backups."""
        file1_name = random_filename()
        dir1_name = random_filename()
        dir2_name = random_filename()
        file1_full_path = os.path.join(dir1_name, file1_name)
        file1_new_full_path = os.path.join(dir2_name, file1_name)

        # create a directory for the file we are going to create
        self.fs.makedir(dir1_name)

        f = self.fs.open(file1_full_path, 'wb')
        f.write("smartfile")
        f.close()

        f = self.fs.open(file1_full_path, 'wb')
        f.write("smartfile versioning")
        f.close()

        # move the directory
        self.fs.movedir(dir1_name, dir2_name)

         # check if versioning is still available
        f = self.fs.open(file1_new_full_path, 'rb', version=2)
        self.assertEqual(f.read(), "smartfile versioning")
        f.close()

    def test_remove_single_file(self):
        """Remove a single file along with its backups."""
        file_name = random_filename()

        f = self.fs.open(file_name, 'wb')
        f.write("smartfile")
        f.close()

        self.fs.remove(file_name)

        self.assertFalse(self.fs.has_snapshot(file_name))

    def test_remove_single_dir(self):
        """Remove a single dir along with its backups."""
        dir_name = random_filename()
        self.fs.makedir(dir_name)

        files = [random_filename() for x in range(4)]
        paths = [os.path.join(dir_name, path) for path in files]

        for path in paths:
            for _ in range(2):
                with self.fs.open(path, 'wb') as f:
                    f.write('hello world')

        self.fs.removedir(dir_name, force=True)

        for path in paths:
            self.assertTrue(not self.fs.has_snapshot(path))


if __name__ == "__main__":
    unittest.main()
