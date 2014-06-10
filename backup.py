import json
import os
import os.path


BASEDIR = os.path.abspath(os.path.dirname(__file__))
BACKUPS_DIR = os.path.join(BASEDIR, 'backups')


class VersioningFS(object):
    def __init__(self, backup_dir, path):
        self.__backup_dir = backup_dir
        self.__path = path

        info_filename = "%s.snapshot" % (hash(path))
        self.__info_path = os.path.join(BACKUPS_DIR, info_filename)

        self.__version = 0
        if os.path.isfile(self.__info_path):
            with open(self.__info_path, mode='r') as f:
                json_data = json.loads(f.read())
                self.__version = json_data['version']

    @property
    def verison(self):
        """
        Returns the current version of the file. Starts at 0.
        """
        return self.__version

    def snapshot(self):
        """
        Creates a snapshot of the file.
        """
        pass
