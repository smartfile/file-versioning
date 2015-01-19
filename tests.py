import os
import random
import string
import unittest

from fs.errors import ResourceNotFoundError
from fs.path import relpath
from fs.tempfs import TempFS
from fs.tests import FSTestCases
from fs.tests import ThreadingTestCases

from versioning_fs import VersioningFS
from versioning_fs.errors import VersionError


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
        rootfs = TempFS()
        backup = TempFS(temp_dir=rootfs.getsyspath('/'))
        self.fs = VersioningFS(rootfs, backup=backup, tmp=TempFS(),
                               testing={'time': 1})

    def tearDown(self):
        self.fs.close()


class BaseTimeSensitiveTest(unittest.TestCase):
    """The base class for tests that should not bypass the time settings for
       rdiff-backup.
    """
    def setUp(self):
        rootfs = TempFS()
        backup = TempFS(temp_dir=rootfs.getsyspath('/'))
        self.fs = VersioningFS(rootfs, backup=backup, tmp=TempFS())

    def tearDown(self):
        self.fs.close()


class TestVersioningFS(FSTestCases, ThreadingTestCases, BaseTimeSensitiveTest):
    maxDiff = None


class TestSnapshotAttributes(BaseTimeSensitiveTest):
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

    def test_file_version_timestamps(self):
        """Test version information for a specific path."""
        file_name = random_filename()
        with self.fs.open(file_name, 'wb') as f:
            f.write('hello world\n')

        self.assertEqual(len(self.fs.list_info(file_name).keys()), 1)

        with self.fs.open(file_name, 'wb') as f:
            f.write('hello world123\n')

        with self.fs.open(file_name, 'wb') as f:
            f.write('hello world123456\n')

        version_info = self.fs.list_info(file_name)

        dates = version_info.values()

        for z in range(len(dates) - 1):
            current_date = dates[z]
            next_date = dates[z+1]
            self.assertTrue(current_date <= next_date)

    def test_file_version_sizes(self):
        """Test version sizes for a specific path."""
        file_name = random_filename()

        for _ in range(3):
            with self.fs.open(file_name, 'wb') as f:
                f.write(random_filename())
                f.write('\n')

        self.assertEqual(len(self.fs.list_sizes(file_name).keys()), 3)

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
    def test_single_file_write(self):
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

        # make sure the version has not been updated since reading
        self.assertEqual(self.fs.version(file_name), 2)

    def test_single_file_append(self):
        file_name = random_filename()

        f = self.fs.open(file_name, 'ab')
        f.write('smartfile_versioning_rocks\n')
        f.close()

        # check that version 1 was created
        self.assertEqual(self.fs.version(file_name), 1)

        f = self.fs.open(file_name, 'rb')
        self.assertEqual(f.read(), 'smartfile_versioning_rocks\n')
        f.close()

        # make some changes to the file and check for version increment
        f = self.fs.open(file_name, 'ab')
        f.writelines("hello world!\nhello world!")
        f.close()
        self.assertEqual(self.fs.version(file_name), 2)

        # check the contents when we open the file
        f = self.fs.open(file_name, 'rb')
        self.assertEqual(f.readlines(), ['smartfile_versioning_rocks\n',
                                         "hello world!\n", "hello world!"])
        f.close()

        # make sure the version has not been updated since reading
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

        # version 0 should never exist
        with self.assertRaises(ResourceNotFoundError):
            self.fs.open(file_name, 'rb', version=0)

        # version 2 has not been created yet
        with self.assertRaises(ResourceNotFoundError):
            self.fs.open(file_name, 'rb', version=2)

    def test_skip_version_snapshot(self):
        """
        Test opening a file but setting 'take_snapshot' to False.
        A version should not be created.
        """
        file_name = random_filename()

        f = self.fs.open(file_name, 'wb', take_snapshot=False)
        f.write('smartfile_versioning_rocks\n')
        f.close()

        # check that version 1 was not created
        self.assertEqual(self.fs.version(file_name), 0)


class TestVersionDeletion(BaseTimeSensitiveTest):
    """Test the deletion of older versions."""
    def test_delete_older_versions(self):
        file_name = random_filename()
        iterations = 5

        # generate some files
        for _ in range(iterations):
            with self.fs.open(file_name, 'wb') as f:
                f.write(random_filename())

        # try a bad version: remove versions before 1
        with self.assertRaises(VersionError):
            self.fs.remove_versions_before(file_name, version=1)

        # try a bad version: remove versions after the current+1
        with self.assertRaises(VersionError):
            invalid_version = iterations + 1
            self.fs.remove_versions_before(file_name, version=invalid_version)

        # try a bad version: use an invalid time format
        with self.assertRaises(VersionError):
            invalid_version = "3/4/1998T13:00"
            self.fs.remove_versions_before(file_name, version=invalid_version)

        # look at the time of version 2 and delete anything older than it
        self.fs.remove_versions_before(path=file_name, version=2)
        # we deleted versions older than 2 which deleted version 1
        total_versions = self.fs.version(file_name)
        self.assertEqual(total_versions, 4)

        # try deleting with a timestamp string rather than version number
        delete_date = self.fs.list_info(file_name)[2]
        self.fs.remove_versions_before(path=file_name, version=delete_date)
        # we deleted versions before the date of the second version
        total_versions = self.fs.version(file_name)
        self.assertEqual(total_versions, 3)

        # try deleting a version with a string that is also a digit
        self.fs.remove_versions_before(path=file_name, version=u'2')
        # we deleted versions older than 2 which deleted version 1
        total_versions = self.fs.version(file_name)
        self.assertEqual(total_versions, 2)


class TestRdiffBackupSleep(BaseTimeSensitiveTest):
    """Rdiff backup cannot make two snapshots within 1 second.
       This test checks that the filewrapper sleeps for 1 second before
       trying to make a snapshot.
    """
    def test_quick_file_changes(self):
        # test two file edits within 1 second
        file_name = random_filename()
        iterations = 3

        for _ in range(iterations):
            with self.fs.open(file_name, 'wb') as f:
                f.write(random_filename())

        self.assertEqual(self.fs.version(file_name), iterations)


class TestFileOperations(BaseTest):
    """Test fs.move, fs.movedir, fs.remove, and fs.removedir"""
    def test_move_single_file(self):
        """Move a single file, which should also move its backups."""
        # have 2 versions of a file we create
        file_name = random_filename()

        contents = ["smartfile", "smartfile versioning"]

        for content in contents:
            with self.fs.open(file_name, 'wb') as f:
                f.write(content)

        # move the file somewhere else
        new_filename = random_filename()
        self.fs.move(file_name, new_filename)

        # check if versioning is still available
        for version, content in enumerate(contents):
            with self.fs.open(new_filename, 'rb', version=version+1) as f:
                self.assertEqual(f.read(), contents[version])

    def test_move_file_into_directory(self):
        """Move a file into a directory and check that backups were moved."""
        file_name = random_filename()
        dir_name = random_filename()
        file_path = os.path.join(dir_name, file_name)

        contents = ["smartfile", "smartfile versioning",
                    "smartfile versioning rocks"]

        for content in contents:
            with self.fs.open(file_name, 'wb') as f:
                f.write(content)

        # create a directory for the file to be moved into
        self.fs.makedir(dir_name)
        # move the file into the directory
        self.fs.move(file_name, file_path)

        # check if versioning is still available
        self.assertTrue(self.fs.has_snapshot(file_path))
        for version, content in enumerate(contents):
            f = self.fs.open(file_path, 'rb', version=version+1)
            self.assertEqual(f.read(), contents[version])
            f.close()

    def test_move_directory(self):
        """Move a directory and check that backups were moved."""
        file1_name = random_filename()
        dir1_name = random_filename()
        dir2_name = random_filename()
        file1_full_path = os.path.join(dir1_name, file1_name)
        file1_new_full_path = os.path.join(dir2_name, file1_name)

        # create a directory for the file we are going to create
        self.fs.makedir(dir1_name)

        contents = ["smartfile", "smartfile versioning"]

        for content in contents:
            with self.fs.open(file1_full_path, 'wb') as f:
                f.write(content)

        # move the directory
        self.fs.movedir(dir1_name, dir2_name)

        # check if versioning is still available
        self.assertTrue(self.fs.has_snapshot(file1_new_full_path))
        for version, content in enumerate(contents):
            f = self.fs.open(file1_new_full_path, 'rb', version=version+1)
            self.assertEqual(f.read(), contents[version])
            f.close()

    def test_rename_file(self):
        """Rename a file and check that backups were moved."""
        file_name = random_filename()
        file2_name = random_filename()

        contents = ["smartfile", "smartfile versioning",
                    "smartfile versioning rocks"]

        for content in contents:
            with self.fs.open(file_name, 'wb') as f:
                f.write(content)

        # Rename the file
        self.fs.rename(file_name, file2_name)

        # check if versioning is still available
        self.assertTrue(self.fs.has_snapshot(file2_name))
        for version, content in enumerate(contents):
            f = self.fs.open(file2_name, 'rb', version=version+1)
            self.assertEqual(f.read(), contents[version])
            f.close()

    def test_rename_directory(self):
        """Rename a directory and check that backups were moved."""
        file1_name = random_filename()
        dir1_name = random_filename()
        dir2_name = random_filename()
        file1_full_path = os.path.join(dir1_name, file1_name)
        file1_new_full_path = os.path.join(dir2_name, file1_name)

        # create a directory for the file we are going to create
        self.fs.makedir(dir1_name)

        contents = ["smartfile", "smartfile versioning"]

        for content in contents:
            with self.fs.open(file1_full_path, 'wb') as f:
                f.write(content)

        # move the directory
        self.fs.rename(dir1_name, dir2_name)

        # check if versioning is still available
        self.assertTrue(self.fs.has_snapshot(file1_new_full_path))
        for version, content in enumerate(contents):
            f = self.fs.open(file1_new_full_path, 'rb', version=version+1)
            self.assertEqual(f.read(), contents[version])
            f.close()

    def test_remove_single_file(self):
        """Remove a single file along with its backups."""
        file_name = random_filename()

        with self.fs.open(file_name, 'wb') as f:
            f.write("smartfile")

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
