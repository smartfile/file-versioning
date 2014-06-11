import json
import os
from Queue import Queue
from subprocess import Popen, PIPE
from threading import Lock

from errors import SnapshotError, SnapshotInfoError


def synchronized(lock):
    """
    Synchronization decorator.

    If there is a task in the Queue(maxsize=1), don't block when trying
    to acquire a lock and skip running the decorated function.

    """

    def wrap(f):
        def sync_function(*args, **kwargs):
            blocking = False
            if snapshot_task_queue.empty():
                snapshot_task_queue.put('1')
                blocking = True
            acquire = lock.acquire(blocking)
            if acquire:
                snapshot_task_queue.get()
            else:
                return None
            try:
                return f(*args, **kwargs)
            finally:
                lock.release()
        return sync_function
    return wrap


snapshot_lock = Lock()
snapshot_task_queue = Queue(maxsize=1)


class VersioningFS(object):
    def __init__(self, user_files, backup_dir, snapshot_info):
        self.__user_files = user_files
        self.__backup = backup_dir
        self.__snapshot_info = snapshot_info

        self.__last_snapshot = None

    def open(self, name, mode, version=None):
        """
        Returns a file-object.

        Parameters
          name (str): A file name relative to the user directory.
          mode (str): The mode for opening the file.
          version (int) (optional): Specifies which version of the file to
            get. If version is set to None, the most recent copy of the file
            will be returned.
        """
        if version is None:
            if self.has_snapshot(name):
                version = self.version(name)
            else:
                abs_path = os.path.join(self.__user_files, name)
                open_file = open(name=abs_path, mode=mode)
                return FileWrapper(fs=self, file_object=open_file)
        else:
            pass

    def has_snapshot(self, path):
        """
        Returns the snapshot status of a path.
        """
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

    @synchronized(snapshot_lock)
    def snapshot(self):
        """
        Takes a snapshot of the user files.
        """
        u_path, b_path = self.__user_files, self.__backup

        command = ['rdiff-backup', '--parsable-output', '--no-eas',
                   '--no-file-statistics', '--no-acls', u_path, b_path]
        process = Popen(command, stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()

        ignore = [lambda x: x.startswith("Warning: could not determine case")]


        if len(stderr) is not 0:
            for rule in ignore:
                if not rule(stderr):
                    raise SnapshotError(stderr)

        for root, dirs, files in os.walk(self.__user_files):
            for filename in files:
                path = os.path.join(root, filename)
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
        """
        Returns the snapshot info file path for a given path
        """
        relative_path = path.lstrip(self.__user_files)
        stripped_path = relative_path.lstrip('./')

        # find where the snapshot info file should be
        info_filename = "%s.info" % (hash(stripped_path))
        info_path = os.path.join(self.__snapshot_info, info_filename)

        return info_path


class FileWrapper(object):
    def __init__(self, fs, file_object, *args, **kwargs):
        self.__fs = fs
        self.__is_modified = False

        self.__file_object = file_object

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
            self.__fs.snapshot()

    def __getattr__(self, attr):
        return getattr(self.__file_object, attr)

    def __iter__(self):
        return self.__getattr__('__iter__')()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.__file_object.close()
