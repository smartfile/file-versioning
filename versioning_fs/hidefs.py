""" Filesystem that can hide a specific directory.
"""
import re

from fs.errors import FSError
from fs.path import abspath, basename, normpath, pathcombine, pathjoin
from fs.wrapfs import WrapFS
import fnmatch


class HideFS(WrapFS):
    """FS wrapper class that hides resources in directory listings.

    The listdir() function takes an extra keyword argument 'hidden'
    indicating whether hidden resources should be included in the output.
    It is False by default.
    """

    def __init__(self, wrapped_fs, hidden_dirs=None):
        super(HideFS, self).__init__(wrapped_fs)
        self.__hidden_dirs = hidden_dirs

    def is_hidden(self, path):
        """Check whether the given path should be hidden."""

        if path.startswith("/"):
            path = path[1:]

        for hidden in self.__hidden_dirs:
            if path.startswith(hidden):
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
            for item in super(HideFS, self).walk(path, wildcard, dir_wildcard,
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
            for item in super(HideFS, self).walkfiles(path, wildcard,
                                                      dir_wildcard, search,
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
            for item in super(HideFS, self).walkdirs(path, wildcard, search,
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

    def listdirinfo(self, path="./", wildcard=None, full=False,
                    absolute=False, dirs_only=False, files_only=False):
        """Retrieves a list of paths and path info under a given path.

        This method behaves like listdir() but instead of just returning
        the name of each item in the directory, it returns a tuple of the
        name and the info dict as returned by getinfo.

        This method may be more efficient than calling
        :py:meth:`~fs.base.FS.getinfo` on each individual item returned
        by :py:meth:`~fs.base.FS.listdir`, particularly for network based
        filesystems.

        :param path: root of the path to list
        :param wildcard: filter paths that match this wildcard
        :param dirs_only: only retrieve directories
        :type dirs_only: bool
        :param files_only: only retrieve files
        :type files_only: bool

        :raises `fs.errors.ResourceNotFoundError`: If the path is not found
        :raises `fs.errors.ResourceInvalidError`: If the path exists, but
        is not a directory

        """
        path = normpath(path)

        def getinfo(p):
            try:
                if full or absolute:
                    return self.getinfo(p)
                else:
                    return self.getinfo(pathjoin(path, p))
            except FSError:
                return {}

        return [(p, getinfo(p))
                for p in self.listdir(path,
                                      wildcard=wildcard,
                                      full=full,
                                      absolute=absolute,
                                      dirs_only=dirs_only,
                                      files_only=files_only)]

    def isdirempty(self, path):
        path = normpath(path)
        iter_dir = iter(self.listdir(path, hidden=True))
        try:
            iter_dir.next()
        except StopIteration:
            return True
        return False
