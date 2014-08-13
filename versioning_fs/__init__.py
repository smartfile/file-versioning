""" Filesystem wrapper that provides versioning capabilities through
    rdiff-backup.
"""
from collections import namedtuple
import hashlib
import os
import random
import shutil
from StringIO import StringIO
from subprocess import Popen, PIPE
import time

from fs.base import synchronize
from fs.filelike import FileWrapper
from fs.errors import OperationFailedError, ResourceNotFoundError
from fs.path import relpath
from fs.tempfs import TempFS

from versioning_fs.errors import SnapshotError, VersionError
from versioning_fs.hidefs import HideFS


hasher = hashlib.sha256  # hashing function to use with backup paths

VersionInfo = namedtuple('VersionInfo', ['timestamp',  'size'])


def hash_path(path):
    """Returns a hash of a given path."""
    safe_path = relpath(path).encode('ascii', 'ignore')
    dest_hash = hasher(safe_path).hexdigest()
    return dest_hash


def is_valid_time_format(timestamp):
    """Verify a timestamp format for compatibility with rdiff-backup."""
    try:
        time.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
        return True
    except ValueError:
        return False


class VersionInfoMixIn(object):
    """MixIn that provides versioning information for a filesystem.
    """

    def has_snapshot(self, path):
        """Returns if a path has a snapshot."""
        if os.path.exists(self.snapshot_snap_path(path)):
            return True
        return False

    def list_versions(self, path):
        """Returns a list of the versions for a file."""
        snap_dir = self.snapshot_snap_path(path)
        command = ['rdiff-backup', '--parsable-output', '-l', snap_dir]
        process = Popen(command, stdout=PIPE, stderr=PIPE)
        stdout = process.communicate()[0]

        versions = []
        listing_file = StringIO(stdout)
        for line in listing_file:
            version_number, _ = line.split()
            versions.append(version_number)

        return sorted(versions)

    def version(self, path):
        """Returns the version of a path."""
        return len(self.list_versions(path))

    def list_info(self, path):
        """Returns a dictionary containing timestamps for each version of a
           path.
        """
        versions = self.list_versions(path)

        def formatted_time(epoch):
            """Convert Unix time into a formatted string that js can read."""
            return time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(epoch))

        info = {k+1: formatted_time(int(v)) for k, v in enumerate(versions)}
        return info

    def list_sizes(self, path):
        """Returns a dictionary containing sizes for each version of a path.
        """
        snap_dir = self.snapshot_snap_path(path)
        command = ['rdiff-backup', '--parsable-output',
                   '--list-increment-sizes', snap_dir]
        process = Popen(command, stdout=PIPE, stderr=PIPE)
        stdout = process.communicate()[0]

        listing_file = StringIO(stdout)
        if len(listing_file.readlines()) < 3:
            return {}

        listing_file.seek(0)

        # skip the first two lines of output
        for _ in range(2):
            next(listing_file)

        # generate a dictionary
        sizes = dict()
        for version, line in enumerate(reversed(listing_file.readlines())):
            size = "%s %s" % (line.split()[5], line.split()[6])
            sizes[version+1] = size

        return sizes


class VersioningFS(VersionInfoMixIn, HideFS):
    """ Versioning filesystem.

        This wraps other filesystems, such as OSFS.
    """
    def __init__(self, fs, backup, tmp, testing=False):
        """
        Parameters
          fs (FS): A filesystem object to be wrapped.
          backup (FS): The filesystem object that mounts the backup directory.
          tmp (FS): The filesystem object used for scratch space when
                restoring older versions of files.
          testing (boolean) (default=False): When testing, it's handy to set
                this to true, since rdiff-backup will prevent the tests
                from taking quick snapshots of a single file.
        """
        hide_abs_path = os.path.split(backup.getsyspath('/'))[0]
        # make sure the backups directory is hidden from the user
        hide = [os.path.basename(hide_abs_path)]
        super(VersioningFS, self).__init__(fs, hide)

        self.__fs = fs
        self.__backup = backup
        self.__tmp = tmp
        self.__testing = testing

    @property
    def fs(self):
        """Returns the FS object that is being wrapped."""
        return self.__fs

    @property
    def backup(self):
        """Returns the FS object of the backup directory."""
        return self.__backup

    @property
    def tmp(self):
        """Returns the FS object for the scratch directory."""
        return self.__tmp

    def close(self, *args, **kwargs):
        self.__fs.close()
        self.__backup.close()
        self.__tmp.close()
        super(VersioningFS, self).close(*args, **kwargs)

    @synchronize
    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None,
             newline=None, line_buffering=False, version=None, **kwargs):
        """
        Returns a file-object. The file-object is wrapped with VersionedFile,
            which will notify VersioningFS to make a snapshot whenever
            the file is changed and closed.

        Parameters
          name (str): A file name relative to the user directory.
          mode (str): The mode for opening the file.
          version (int) (optional): Specifies which version of the file to
            get. If version is set to None, the most recent copy of the file
            will be returned.
        """
        path = relpath(path)
        if version is None:
            instance = super(VersioningFS, self)
            file_object = instance.open(path=path, mode=mode,
                                        buffering=buffering, errors=errors,
                                        newline=newline,
                                        line_buffering=line_buffering,
                                        **kwargs)
            return VersionedFile(fs=self, file_object=file_object, mode=mode,
                                 path=path)
        else:
            if version < 1:
                raise ResourceNotFoundError("Version %s not found" %
                                            (version))
            if version == self.version(path):
                instance = super(VersioningFS, self)
                file_object = instance.open(path=path, mode=mode,
                                            buffering=buffering,
                                            errors=errors, newline=newline,
                                            line_buffering=line_buffering,
                                            **kwargs)
                return VersionedFile(fs=self, file_object=file_object,
                                     mode=mode, temp_file=False, path=path)

            snap_dir = self.snapshot_snap_path(path)

            sorted_versions = self.list_versions(path)
            if version > len(sorted_versions):
                raise ResourceNotFoundError("Version %s not found" %
                                            (version))

            requested_version = sorted_versions[version-1]
            if "w" not in mode:
                temp_name = '%020x' % random.randrange(16**30)
                dest_path = os.path.join(self.tmp.getsyspath('/'), temp_name)
                command = ['rdiff-backup',
                           '--restore-as-of', requested_version,
                           snap_dir, dest_path]
                process = Popen(command, stdout=PIPE, stderr=PIPE)
                process.communicate()

                file_path = os.path.join(temp_name, 'datafile')
                open_file = self.tmp.open(file_path, mode=mode)
                return VersionedFile(fs=self, file_object=open_file,
                                     mode=mode, temp_file=True,
                                     path=file_path, remove=dest_path)

    def remove(self, path):
        """Remove a file from the filesystem."""
        super(VersioningFS, self).remove(path)
        self.__delete_snapshot(path)

    def removedir(self, path, recursive=False, force=False):
        if self.fs.isdirempty(path) or force:
            rel_path = relpath(path)
            for filename in self.fs.walkfiles(rel_path):
                self.__delete_snapshot(filename)

        super(VersioningFS, self).removedir(path, recursive, force)

    def __delete_snapshot(self, path):
        """Deletes a snapshot for a given path."""
        if self.has_snapshot(path):
            snap_dest_dir = self.snapshot_snap_path(path)
            shutil.rmtree(snap_dest_dir)

    def move(self, src, dst, *args, **kwargs):
        """Move a file from one place to another."""

        # move the file
        super(VersioningFS, self).move(src, dst, *args, **kwargs)
        self.__move_snapshot(src, dst)

    def movedir(self, src, dst, *args, **kwargs):
        """Move a directory from one place to another."""

        # first, move the backups
        rel_src = relpath(src)
        rel_dst = relpath(dst)
        for path in self.fs.walkfiles(rel_src):
            if self.has_snapshot(path):
                new_path = path.replace(rel_src, rel_dst)

                old_abs_path = self.snapshot_snap_path(path)
                new_abs_path = self.snapshot_snap_path(new_path)

                os.rename(old_abs_path, new_abs_path)

        super(VersioningFS, self).movedir(src, dst, *args, **kwargs)

    def rename(self, src, dst):
        """Rename a file."""

        # rename the file
        super(VersioningFS, self).rename(src, dst)
        self.__move_snapshot(src, dst)

    def __move_snapshot(self, src, dst):
        """Move the snapshot associated with a file."""
        if self.has_snapshot(src):
            src_snapshot = self.snapshot_snap_path(src)
            dst_snapshot = self.snapshot_snap_path(dst)
            if os.path.exists(dst_snapshot):
                shutil.rmtree(dst_snapshot)
            shutil.move(src_snapshot, dst_snapshot)

    @synchronize
    def snapshot(self, path):
        """Takes a snapshot of an individual file."""

        # try grabbing the temp filesystem system path
        temp_dir = None
        if 'getsyspath' in dir(self.tmp):
            temp_dir = self.tmp.getsyspath('/')

        # Create a temp file system to be snapshotted
        temp_snapshot_fs = TempFS(temp_dir=temp_dir)
        src_path = temp_snapshot_fs.getsyspath('/')

        with self.fs.open(path, 'rb') as source_file:
            with temp_snapshot_fs.open('datafile', 'wb') as temp_file:
                shutil.copyfileobj(source_file, temp_file)

         # snapshot destination directory
        dest_dir = self.snapshot_snap_path(path)

        command = ['rdiff-backup', '--parsable-output', '--no-eas',
                   '--no-file-statistics', '--no-acls', src_path, dest_dir]

        # speed up the tests
        if self.__testing:
            command.insert(5, '--current-time')
            command.insert(6, str(self.__testing['time']))
            self.__testing['time'] += 1

        process = Popen(command, stdout=PIPE, stderr=PIPE)
        stderr = process.communicate()[1]

        ignore = [lambda x: x.startswith("Warning: could not determine case")]

        if len(stderr) is not 0:
            for rule in ignore:
                if not rule(stderr):
                    raise SnapshotError(stderr)

        # close the temp snapshot filesystem
        temp_snapshot_fs.close()

    def remove_versions_before(self, path, version):
        """Removes snapshots before a specified version.

           The specified version can be either a version (int) or a time (str)
           in the following format: '%Y-%m-%dT%H:%M:%S'
        """
        if not self.exists(path):
            raise ResourceNotFoundError(path)

        if not self.isfile(path):
            raise OperationFailedError(path)

        # if the version number is a string, try converting it into an int
        if isinstance(version, str) or isinstance(version, unicode):
            try:
                if str(version).isdigit():
                    version = int(version)
            except ValueError:
                raise VersionError("Invalid version.")

        if isinstance(version, int):
            current_version = self.version(path)
            # Versions can't be deleted before version 1 or after the current
            if version > current_version or version <= 1:
                raise VersionError("Invalid version.")

            date_to_delete = self.list_info(path)[version]
        else:
            # check for an invalid timestamp string
            if not is_valid_time_format(version):
                raise VersionError("Invalid time format.")

            date_to_delete = version

        snap_dir = self.snapshot_snap_path(path)
        command = ['rdiff-backup', '--parsable-output', '--force',
                   '--remove-older-than', str(date_to_delete), snap_dir]
        process = Popen(command, stdout=PIPE, stderr=PIPE)
        stderr = process.communicate()[1]

        if len(stderr) > 0:
            raise OperationFailedError(path)

    def snapshot_info_path(self, path):
        """Returns the snapshot info file path for a given path."""

        path = relpath(path)
        # find where the snapshot info file should be
        dest_hash = hash_path(path)
        info_filename = "%s.info" % (dest_hash)
        info_path = os.path.join(self.__tmp.getsyspath('/'), info_filename)

        return info_path

    def snapshot_snap_path(self, path):
        """Returns the dir containing the snapshots for a given path."""

        path = relpath(path)
        dest_hash = hash_path(path)

        backup_dir = self.backup.getsyspath('/')
        save_snap_dir = os.path.join(backup_dir, dest_hash)
        return save_snap_dir


class VersionedFile(FileWrapper):
    """File wrapper that notifies the versioning filesystem to take a
       snapshot if the file has been modified.
    """
    def __init__(self, file_object, mode, fs, path, temp_file=False,
                 remove=None):
        super(VersionedFile, self).__init__(file_object, mode)
        self.__fs = fs
        self.__path = path
        self._is_temp_file = temp_file
        self.__is_modified = False

        self.__file_object = file_object
        self.__remove = remove

    def _write(self, *args, **kwargs):
        self.__is_modified = True
        return super(VersionedFile, self)._write(*args, **kwargs)

    def writelines(self, *args, **kwargs):
        self.__is_modified = True
        return super(VersionedFile, self).writelines(*args, **kwargs)

    def close(self):
        """Close the file and make a snapshot if the file was modified.
        """
        super(VersionedFile, self).close()

        if self._is_temp_file:
            remove = os.path.join(self.__fs.tmp, self.__remove)
            shutil.rmtree(remove)

        if self.__is_modified:
            max_tries = 3  # limit the amount of tries to make a snapshot

            for _ in range(max_tries):
                try:
                    self.__fs.snapshot(self.__path)
                except SnapshotError:
                    # rdiff-backup must wait 1 second between the same file.
                    time.sleep(1)
                else:
                    break
