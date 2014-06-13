from datetime import datetime
import json
import logging
import os
import random
import re
import shutil
from subprocess import Popen, PIPE

from fs.base import FS
from fs.errors import ResourceInvalidError, ResourceNotFoundError

from errors import SnapshotError, SnapshotInfoError


class VersioningFS(FS):
    def __init__(self, user_files, backup_dir, snapshot_info, tmp):
        self.__logger = logging.getLogger('versioningfs')

        self._user_files = user_files
        self.__backup = backup_dir
        self.__snapshot_info = snapshot_info
        self._tmp = tmp

        self.__last_snapshot = None

    def open(self, path, mode, version=None):
        """
        Returns a file-object. The file-object is wrapped with FileWrapper,
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
            abs_path = os.path.join(self._user_files, path)
            open_file = open(name=abs_path, mode=mode)
            return FileWrapper(fs=self, file_object=open_file)
        else:
            if version == self.version(path):
                abs_path = os.path.join(self._user_files, path)
                open_file = open(name=abs_path, mode=mode)
                return FileWrapper(fs=self, file_object=open_file)

            snap_dest_dir = self.__snapshot_snap_path(path)
            increments_dir = os.path.join(snap_dest_dir,
                                          "rdiff-backup-data/increments")

            def dt_parse(date_string):
                date_string = date_string[::-1].split('-', 1)[1][::-1]
                dt = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')
                return dt

            versions = dict()

            for root, dirs, filenames in os.walk(increments_dir):
                for name in filenames:
                    search = re.search(r'(^([^.]*).*)', name, re.M|re.I)

                    filename = search.group(2)
                    stripped_filename = name.lstrip("%s" % (filename))
                    remove_chars = stripped_filename.replace(".", "")
                    time = remove_chars.rstrip(".diff.gz")
                    versions[dt_parse(time)] = os.path.join(root, name)

            sorted_versions = sorted(versions.iterkeys())
            if version > len(sorted_versions) - 1:
                raise ResourceNotFoundError("Version %s not found" % \
                                            (version))

            requested_path = versions[sorted_versions[version]]

            if mode == "r" or mode == "rb":
                src_path = requested_path

                temp_name = '%020x' % random.randrange(16**30)
                dest_path = os.path.join(self._tmp, temp_name)
                command = ['rdiff-backup', '--parsable-output', src_path,
                           dest_path]
                process = Popen(command, stdout=PIPE, stderr=PIPE)
                stdout, stderr = process.communicate()

                open_file = open(name=dest_path, mode=mode)
                return FileWrapper(fs=self, file_object=open_file,
                                   temp_file=True)

    def remove(self, path):
        """Remove a file from the filesystem.

        :param path: Path of the resource to remove
        :type path: string

        :raises `fs.errors.ResourceInvalidError`: if the path is a directory
        :raises `fs.errors.ResourceNotFoundError`: if the path does not exist

        """
        user_file = os.path.join(self._user_files, path)
        if os.path.isdir(user_file):
            raise ResourceInvalidError(path)
        if not os.path.exists(user_file):
            raise ResourceNotFoundError(path)

        os.remove(user_file)

        snap_source_dir = self.__snapshot_source(path)
        shutil.rmtree(snap_source_dir)

        if self.has_snapshot(path):
            snap_dest_dir = self.__snapshot_snap_path(path)
            info_path = self.__snapshot_info_path(path)

            shutil.rmtree(snap_dest_dir)
            os.remove(info_path)

    def has_snapshot(self, path):
        """Returns the snapshot status of a path."""

        info_path = self.__snapshot_info_path(path)

        if os.path.isfile(info_path):
            return True

        return False

    def version(self, path):
        """
        Returns the current version of a given file. Starts at 0.
        """
        info_path = self.__snapshot_info_path(path)

        if os.path.isfile(info_path):
            with open(info_path, 'rb') as f:
                json_data = json.loads(f.read())
                version = json_data['version']
        else:
            raise SnapshotInfoError("There is no snapshot information for "
                                    "the given path.")

        return version

    def snapshot(self, path):
        """
        Takes a snapshot of an individual file.
        """

        snap_source_dir = self.__snapshot_source(path)
        snap_dest_dir = self.__snapshot_snap_path(path)

        if not self.has_snapshot(path):
            os.makedirs(snap_source_dir)
            os.makedirs(snap_dest_dir)

            # create a hard link inside of a folder within the snapshot dir
            link_src = os.path.join(self._user_files, path)
            link_dst = os.path.join(snap_source_dir, str(hash(path)))
            os.link(link_src, link_dst)

        src_path, dest_path = snap_source_dir, snap_dest_dir

        command = ['rdiff-backup', '--parsable-output', '--no-eas',
                   '--no-file-statistics', '--no-acls', src_path, dest_path]
        process = Popen(command, stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()

        ignore = [lambda x: x.startswith("Warning: could not determine case")]


        if len(stderr) is not 0:
            for rule in ignore:
                if not rule(stderr):
                    raise SnapshotError(stderr)

        # update the version of the file
        self._update_version(path)


    def _update_version(self, path):
        """
        Increases the version number for a given path.
        """
        info_path = self.__snapshot_info_path(path)

        # if a snapshot file exists, try reading the version info
        version = -1
        if os.path.isfile(info_path):
            with open(info_path, 'rb') as f:
                json_data = json.loads(f.read())
                version = json_data['version']

        # increase the version number
        version += 1

        # write the version info back to the snapshot info file
        with open(info_path, 'wb') as f:
            data = {'version': version}
            json_data = json.dumps(data)
            f.write(json_data)

    def __snapshot_info_path(self, path):
        """Returns the snapshot info file path for a given path."""

        relative_path = path.lstrip(self._user_files)
        stripped_path = relative_path.replace("./", "", 1)

        # find where the snapshot info file should be
        info_filename = "%s.info" % (hash(stripped_path))
        info_path = os.path.join(self.__snapshot_info, info_filename)

        return info_path

    def __snapshot_snap_path(self, path):
        """Returns the dir containing the snapshots for a given path."""

        save_snap_dir = os.path.join(self.__backup, str(hash(path)))
        return save_snap_dir

    def __snapshot_source(self, path):
        """Returns the dir of the file to be snapshotted. This dir should
        contain a hardlink to the original file in the user files directory.
        """

        snap_dir = "%s.backup" % (self.__snapshot_info_path(path))
        return snap_dir


class FileWrapper(object):
    def __init__(self, fs, file_object, temp_file=False, *args, **kwargs):
        self.__fs = fs
        self.__temp_file = temp_file
        self.__is_modified = False

        self.__file_object = file_object
        path = self.__file_object.name.replace(self.__fs._user_files,
                                                     "")
        self._path = path.replace("/", "")

    def write(self, *args, **kwargs):
        self.__file_object.write(*args, **kwargs)
        self.__is_modified = True

    def writelines(self, *args, **kwargs):
        self.__file_object.writelines(*args, **kwargs)
        self.__is_modified = True

    def close(self):
        """
        Close the file and make a snapshot if the file was modified.
        """
        self.__file_object.close()

        if self.__is_modified:
            self.__fs.snapshot(self._path)

        if self.__temp_file:
            os.remove(self.__file_object.name)

    def __getattr__(self, attr):
        return getattr(self.__file_object, attr)

    def __iter__(self):
        return self.__getattr__('__iter__')()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()
