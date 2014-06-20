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


class VersionManager(object):
    """
    Base class for managing versions of files.
    """
    def version(self, path):
        raise NotImplementedError

    def set_version(self, path, version):
        raise NotImplementedError

    def remove(self, path):
        raise NotImplementedError

    def _update_version(self, path):
        version = self.version(path)
        if version is None:
            version = 0
        version += 1
        self.set_version(path, version)


class VersioningFS(HideBackupFS):
    """ Versioning filesystem.

        This wraps other filesystems, such as OSFS.
    """
    def __init__(self, fs, version_manager, backup_dir, tmp, testing=False):
        super(VersioningFS, self).__init__(fs, backup_dir)

        self.__fs = fs
        self._v_manager = version_manager
        self.__backup = backup_dir
        self._tmp = os.path.join(tmp, 'versioningfs')
        self.__testing = testing

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
            f = super(VersioningFS, self).open(path=path, mode=mode,
                                               buffering=buffering,
                                               errors=errors, newline=newline,
                                               line_buffering=line_buffering,
                                               **kwargs)
            return VersionedFile(fs=self, file_object=f, mode=mode,
                                 path=path)
        else:
            if version < 1:
                raise ResourceNotFoundError("Version %s not found" %
                                            (version))
            if version == self._v_manager.version(path):
                f = super(VersioningFS, self).open(path=path, mode=mode,
                                               buffering=buffering,
                                               errors=errors, newline=newline,
                                               line_buffering=line_buffering,
                                               **kwargs)
                return VersionedFile(fs=self, file_object=f, mode=mode,
                                     temp_file=False, path=path)

            snap_dir = self.__snapshot_snap_path(path)
            command = ['rdiff-backup', '--parsable-output', '-l', snap_dir]
            process = Popen(command, stdout=PIPE, stderr=PIPE)
            stdout, stderr = process.communicate()

            versions = []
            listing_file = StringIO(stdout)
            for line in listing_file:
                timestamp, kind = line.split()
                versions.append(timestamp)

            sorted_versions = sorted(versions)
            if version > len(sorted_versions):
                raise ResourceNotFoundError("Version %s not found" %
                                            (version))

            requested_version = sorted_versions[version-1]
            if "w" not in mode:
                temp_name = '%020x' % random.randrange(16**30)
                dest_path = os.path.join(self._tmp, temp_name)
                command = ['rdiff-backup',
                           '--restore-as-of', requested_version,
                           snap_dir, dest_path]
                process = Popen(command, stdout=PIPE, stderr=PIPE)
                stdout, stderr = process.communicate()

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
        if self.__fs.isdir(path):
            raise ResourceInvalidError(path)
        if not self.__fs.exists(path):
            raise ResourceNotFoundError(path)

        self.__fs.remove(path)

        if self._v_manager.has_snapshot(path):
            snap_dest_dir = self.__snapshot_snap_path(path)
            shutil.rmtree(snap_dest_dir)

    @synchronize
    def snapshot(self, path):
        """
        Takes a snapshot of an individual file.
        """

        # relative to the mounted fs, what should be snapshotted and where
        # should it go
        snap_source_dir = self.__snapshot_source(path)
        snap_dest_dir = self.__snapshot_snap_path(path)

        # create the directory where the snapshot will be taken from
        os.makedirs(snap_source_dir)
        if not self._v_manager.has_snapshot(path):
            os.makedirs(snap_dest_dir)

        link_src = self.__fs.getsyspath(path)

        dest_hash = hashlib.sha256(path).hexdigest()
        link_dst = os.path.join(snap_source_dir, dest_hash)

        # hardlink the user file to a file inside a temp dir
        os.link(link_src, link_dst)

        src_path = os.path.join(self._tmp, snap_source_dir)
        dest_path = snap_dest_dir

        command = ['rdiff-backup', '--parsable-output', '--no-eas',
                   '--no-file-statistics', '--no-acls', src_path, dest_path]

        # speedup the tests
        if self.__testing:
            command.insert(5, '--current-time')
            command.insert(6, str(self.__testing['time']))
            self.__testing['time'] += 1

        process = Popen(command, stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()

        ignore = [lambda x: x.startswith("Warning: could not determine case")]

        if len(stderr) is not 0:
            for rule in ignore:
                if not rule(stderr):
                    raise SnapshotError(stderr)

        # update the version of the file
        self._v_manager._update_version(path)

        shutil.rmtree(snap_source_dir)

    def __snapshot_info_path(self, path):
        """Returns the snapshot info file path for a given path."""

        # find where the snapshot info file should be
        dest_hash = hashlib.sha256(path).hexdigest()
        info_filename = "%s.info" % (dest_hash)
        info_path = os.path.join(self._tmp, info_filename)

        return info_path

    def __snapshot_snap_path(self, path):
        """Returns the dir containing the snapshots for a given path."""

        dest_hash = hashlib.sha256(path).hexdigest()

        backup_dir = self.__fs.getsyspath(self.__backup)
        save_snap_dir = os.path.join(backup_dir, dest_hash)
        return save_snap_dir

    def __snapshot_source(self, path):
        """Returns the dir of the file to be snapshotted. This dir should
        contain a hardlink to the original file in the user files directory.
        """

        snap_dir = "%s.backup" % (self.__snapshot_info_path(path))
        return snap_dir


class VersionedFile(FileWrapper):
    def __init__(self, file_object, mode, fs, path, temp_file=False,
                 remove=None, *args, **kwargs):
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
        """
        Close the file and make a snapshot if the file was modified.
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
            remove = os.path.join(self.__fs._tmp, self.__remove)
            shutil.rmtree(remove)
