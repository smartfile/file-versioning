""" Filesystem that can hide a specific directory.
"""
import re

from fs.wrapfs import WrapFS
from fs.path import abspath, basename, normpath, pathcombine
import fnmatch


class HideBackupFS(WrapFS):
    """FS wrapper class that hides backups in directory listings.

    The listdir() function takes an extra keyword argument 'hidden'
    indicating whether hidden backups should be included in the output.
    It is False by default.
    """

    def __init__(self, wrapped_fs, hidden_dir=None):
        super(HideBackupFS, self).__init__(wrapped_fs)
        self.__hidden_dir = hidden_dir

    def is_hidden(self, path):
        """Check whether the given path should be hidden."""
        if path.startswith(self.__hidden_dir):
            return True
        if path.startswith("/%s" % self.__hidden_dir):
            return True
        return False

    def _encode(self, path):
        return path

    def _decode(self, path):
        return path

    def listdir(self, path="", wildcard=None, full=False, absolute=False,
                dirs_only=False, files_only=False, hidden=False):
        kwds = dict(wildcard=wildcard,
                    full=full,
                    absolute=absolute,
                    dirs_only=dirs_only,
                    files_only=files_only)
        entries = self.wrapped_fs.listdir(path, **kwds)
        if not hidden:
            entries = [e for e in entries if not self.is_hidden(e)]
        return entries

    def ilistdir(self, path="", wildcard=None, full=False, absolute=False,
                 dirs_only=False, files_only=False, hidden=False):
        kwds = dict(wildcard=wildcard,
                    full=full,
                    absolute=absolute,
                    dirs_only=dirs_only,
                    files_only=files_only)
        for e in self.wrapped_fs.ilistdir(path, **kwds):
            if hidden or not self.is_hidden(e):
                yield e

    def walk(self, path="/", wildcard=None, dir_wildcard=None,
             search="breadth", ignore_errors=False):
        if dir_wildcard is not None:
            #  If there is a dir_wildcard, fall back to the default impl
            #  that uses listdir().  Otherwise we run the risk of enumerating
            #  lots of directories that will just be thrown away.
            for item in super(HideBackupFS, self).walk(path, wildcard,
                                                       dir_wildcard,
                                                       search, ignore_errors):
                yield item
        #  Otherwise, the wrapped FS may provide a more efficient impl
        #  which we can use directly.
        else:
            if wildcard is not None and not callable(wildcard):
                wildcard_re = re.compile(fnmatch.translate(wildcard))
                wildcard = lambda fn: bool(wildcard_re.match(fn))
            walk = self.wrapped_fs.walk(self._encode(path), search=search,
                                        ignore_errors=ignore_errors)
            for (dirpath, filepaths) in walk:
                if self.is_hidden(dirpath):
                    continue
                filepaths = [basename(self._decode(pathcombine(dirpath, p)))
                             for p in filepaths]
                dirpath = abspath(self._decode(dirpath))
                if wildcard is not None:
                    filepaths = [p for p in filepaths if wildcard(p)]
                yield (dirpath, filepaths)

    def walkfiles(self, path="/", wildcard=None, dir_wildcard=None,
                  search="breadth", ignore_errors=False):
        if dir_wildcard is not None:
            #  If there is a dir_wildcard, fall back to the default impl
            #  that uses listdir().  Otherwise we run the risk of enumerating
            #  lots of directories that will just be thrown away.
            for item in super(HideBackupFS, self).walkfiles(path, wildcard,
                                                            dir_wildcard,
                                                            search,
                                                            ignore_errors):
                yield item
        #  Otherwise, the wrapped FS may provide a more efficient impl
        #  which we can use directly.
        else:
            if wildcard is not None and not callable(wildcard):
                wildcard_re = re.compile(fnmatch.translate(wildcard))
                wildcard = lambda fn: bool(wildcard_re.match(fn))
            walk = self.wrapped_fs.walkfiles(self._encode(path),
                                             search=search,
                                             ignore_errors=ignore_errors)
            for filepath in walk:
                filepath = abspath(self._decode(filepath))
                if wildcard is not None:
                    if not wildcard(basename(filepath)):
                        continue
                if self.is_hidden(filepath):
                    continue
                yield filepath

    def walkdirs(self, path="/", wildcard=None, search="breadth",
                 ignore_errors=False):
        if wildcard is not None:
            #  If there is a wildcard, fall back to the default impl
            #  that uses listdir().  Otherwise we run the risk of enumerating
            #  lots of directories that will just be thrown away.
            for item in super(HideBackupFS, self).walkdirs(path, wildcard,
                                                           search,
                                                           ignore_errors):
                yield item
        #  Otherwise, the wrapped FS may provide a more efficient impl
        #  which we can use directly.
        else:
            walk = self.wrapped_fs.walkdirs(self._encode(path), search=search,
                                            ignore_errors=ignore_errors)
            for dirpath in walk:
                if self.is_hidden(dirpath):
                    continue
                yield abspath(self._decode(dirpath))

    def isdirempty(self, path):
        path = normpath(path)
        iter_dir = iter(self.listdir(path, hidden=True))
        try:
            iter_dir.next()
        except StopIteration:
            return True
        return False
