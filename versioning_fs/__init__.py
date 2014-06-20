""" Filesystem wrapper that provides versioning capabilities through
    rdiff-backup.
"""
import hashlib
import os
import random
import shutil
from StringIO import StringIO
from subprocess import Popen, PIPE
import time

from fs.base import synchronize
from fs.filelike import FileWrapper
from fs.errors import ResourceInvalidError, ResourceNotFoundError


from versioning_fs.errors import SnapshotError
from versioning_fs.hidebackupfs import HideBackupFS


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


class VersioningFS(VersionInfoMixIn, HideBackupFS):
    """ Versioning filesystem.

        This wraps other filesystems, such as OSFS.
    """
    def __init__(self, fs, backup_dir, tmp, testing=False):
        super(VersioningFS, self).__init__(fs, backup_dir)

        self.fs = fs
        self.backup = backup_dir
        self.__tmp = tmp
        self.__testing = testing

    @property
    def tmp(self):
        return self.__tmp

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
                dest_path = os.path.join(self.__tmp, temp_name)
                command = ['rdiff-backup',
                           '--restore-as-of', requested_version,
                           snap_dir, dest_path]
                process = Popen(command, stdout=PIPE, stderr=PIPE)
                process.communicate()

                dest_hash = hashlib.sha256(path).hexdigest()

                file_path = os.path.join(dest_path, dest_hash)
                open_file = open(name=file_path, mode=mode)
                return VersionedFile(fs=self, file_object=open_file,
                                     mode=mode, temp_file=True,
                                     path=file_path, remove=dest_path)

    def remove(self, path):
        """Remove a file from the filesystem.

        :param path: Path of the resource to remove
        :type path: string

        :raises `fs.errors.ResourceInvalidError`: if the path is a directory
        :raises `fs.errors.ResourceNotFoundError`: if the path does not exist

        """
        if self.fs.isdir(path):
            raise ResourceInvalidError(path)
        if not self.fs.exists(path):
            raise ResourceNotFoundError(path)

        self.fs.remove(path)

        if self.has_snapshot(path):
            snap_dest_dir = self.snapshot_snap_path(path)
            shutil.rmtree(snap_dest_dir)

    @synchronize
    def snapshot(self, path):
        """Takes a snapshot of an individual file."""

        # relative to the mounted fs, what should be snapshotted and where
        # should it go
        snap_source_dir = self.snapshot_source(path)
        snap_dest_dir = self.snapshot_snap_path(path)

        # create the directory where the snapshot will be taken from
        os.makedirs(snap_source_dir)
        if not self.has_snapshot(path):
            os.makedirs(snap_dest_dir)

        link_src = self.fs.getsyspath(path)

        dest_hash = hashlib.sha256(path).hexdigest()
        link_dst = os.path.join(snap_source_dir, dest_hash)

        # hardlink the user file to a file inside a temp dir
        os.link(link_src, link_dst)

        src_path = os.path.join(self.__tmp, snap_source_dir)
        dest_path = snap_dest_dir

        command = ['rdiff-backup', '--parsable-output', '--no-eas',
                   '--no-file-statistics', '--no-acls', src_path, dest_path]

        # speedup the tests
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

        # remove  the intermediate directory
        shutil.rmtree(snap_source_dir)

    def snapshot_info_path(self, path):
        """Returns the snapshot info file path for a given path."""

        # find where the snapshot info file should be
        dest_hash = hashlib.sha256(path).hexdigest()
        info_filename = "%s.info" % (dest_hash)
        info_path = os.path.join(self.__tmp, info_filename)

        return info_path

    def snapshot_snap_path(self, path):
        """Returns the dir containing the snapshots for a given path."""

        dest_hash = hashlib.sha256(path).hexdigest()

        backup_dir = self.fs.getsyspath(self.backup)
        save_snap_dir = os.path.join(backup_dir, dest_hash)
        return save_snap_dir

    def snapshot_source(self, path):
        """Returns the dir of the file to be snapshotted. This dir should
           contain a hardlink to the original file in the user files
           directory.
        """

        snap_dir = "%s.backup" % (self.snapshot_info_path(path))
        return snap_dir


class VersionedFile(FileWrapper):
    """File wrapper that notifies the versioning filesystem to take a
       snapshot if the file has been modified.
    """
    def __init__(self, file_object, mode, fs, path, temp_file=False,
                 remove=None):
        super(VersionedFile, self).__init__(file_object, mode)
        self.__fs = fs
        self.__path = path
        self.__temp_file = temp_file
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

        if self.__is_modified:
            try:
                self.__fs.snapshot(self.__path)
            except SnapshotError:
                # rdiff-backup must wait 1 second between the same file
                time.sleep(1)
                self.__fs.snapshot(self.__path)

        if self.__temp_file:
            remove = os.path.join(self.__fs.tmp, self.__remove)
            shutil.rmtree(remove)
