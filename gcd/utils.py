import functools


def unroll(fn):
    @functools.wraps(fn)
    def _wrap(*args, **kwargs):
        return list(fn(*args, **kwargs))

    return _wrap
