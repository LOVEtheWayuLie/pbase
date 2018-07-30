"""
HappyBase utility module.

These functions are not part of the public API.
"""
import six

try:
    # Python 2.7 and up
    from collections import OrderedDict
except ImportError:
    try:
        # External package for Python 2.6
        from ordereddict import OrderedDict
    except ImportError as exc:
        # Stub to throw errors at run-time (not import time)
        def OrderedDict(*args, **kwargs):
            raise RuntimeError(
                "No OrderedDict implementation available; please "
                "install the 'ordereddict' Package from PyPI.")


def make_to_dict(item, include_timestamp):
    """Make a row dict for a cell mapping like ttypes.TResult.columns."""
    return {
        '%s:%s' % (cell.family, cell.qualifier): (cell.value, cell.timestamp) if include_timestamp else cell.value
            for cell in item
    }


def make_ordered_to_dict(sorted_columns, include_timestamp):
    """Make a row dict for sorted column results from scans."""
    od = OrderedDict()
    for column in sorted_columns:
        if include_timestamp:
            value = (column.cell.value, column.cell.timestamp)
        else:
            value = column.cell.value
        od[column.columnName] = value
    return od


def ensure_bytes(str_or_bytes, binary_type=six.binary_type,
                 text_type=six.text_type):
    """Convert text into bytes, and leaves bytes as-is."""
    if isinstance(str_or_bytes, binary_type):
        return str_or_bytes
    if isinstance(str_or_bytes, text_type):
        return str_or_bytes.encode('utf-8')
    raise TypeError(
        "input must be a text or byte string, got {}"
        .format(type(str_or_bytes).__name__))


def bytes_increment(b):
    """Increment and truncate a byte string (for sorting purposes)

    This functions returns the shortest string that sorts after the given
    string when compared using regular string comparison semantics.

    This function increments the last byte that is smaller than ``0xFF``, and
    drops everything after it. If the string only contains ``0xFF`` bytes,
    `None` is returned.
    """
    assert isinstance(b, six.binary_type)
    b = bytearray(b)  # Used subset of its API is the same on Python 2 and 3.
    for i in range(len(b) - 1, -1, -1):
        if b[i] != 0xff:
            b[i] += 1
            return bytes(b[:i+1])
    return None