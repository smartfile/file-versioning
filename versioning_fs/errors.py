""" Errors related to the versioning filesystem.
"""


class BaseError(Exception):
    """Base error class."""
    def __init__(self, message, *args, **kwargs):
        self.message = message
        self.details = kwargs
        super(BaseError, self).__init__(*args, **kwargs)

    def __str__(self):
        return repr(self.message)


class SnapshotError(BaseError):
    """Raised when rdiff-backup returns an error."""
    def __init__(self, *args, **kwargs):
        super(SnapshotError, self).__init__(*args, **kwargs)


class VersionError(BaseError):
    """Raised when an invalid file version is requested."""
    def __init__(self, *args, **kwargs):
        super(VersionError, self).__init__(*args, **kwargs)
