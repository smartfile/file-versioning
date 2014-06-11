class BaseError(Exception):
    def __init__(self, message, *args, **kwargs):
        self.message = message
        self.details = kwargs

    def __str__(self):
        return repr(self.message)


class SnapshotError(BaseError):
    def __init__(self, *args, **kwargs):
        super(SnapshotError, self).__init__(*args, **kwargs)


class SnapshotInfoError(BaseError):
    def __init__(self, *args, **kwargs):
        super(SnapshotInfoError, self).__init__(*args, **kwargs)
