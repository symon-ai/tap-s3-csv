import gzip
import ntpath
import posixpath
import struct


def _sanitize_gz_inner_filename(name):
    """Sanitize a filename read from a gzip FNAME header.

    The inner filename originates from user-supplied gzip data and is later
    concatenated into a path (e.g. ``s3_path + "/" + gz_file_name``). Per
    RFC 1952 the FNAME field is the original name with directory components
    removed, so any embedded path separators (``/`` or ``\\``) or parent
    references (``..``) indicate path manipulation (CWE-73). Strip every
    directory component and reject traversal so only a bare filename remains.
    """
    if not name:
        return name

    # Collapse both POSIX and Windows separators to their basename so that a
    # crafted header such as "../../etc/passwd.csv" cannot escape the intended
    # location.
    base = posixpath.basename(ntpath.basename(name))

    # After stripping directory components a residual "." / ".." (or empty)
    # value is not a usable filename and must not be trusted.
    if base in ("", ".", ".."):
        return None

    return base


def get_file_name_from_gzfile(filename=None, fileobj=None):
    """Reading headers of GzipFile and returning filename."""
    _gz = gzip.GzipFile(filename=filename,fileobj=fileobj)
    _fp = _gz.fileobj

    # the magic 2 bytes: if 0x1f 0x8b (037 213 in octal)
    magic = _fp.read(2)
    if magic == b'':
        return None

    if magic != b'\037\213':
        raise OSError('Not a gzipped file (%r)' % magic)

    (method, flag, _) = struct.unpack("<BBIxx", _read_exact(_fp, 8))
    if method != 8:
        raise OSError('Unknown compression method')

    # Case where the name is not in the header according to flag
    if not flag & gzip.FNAME:
        # Not stored in the header, use the filename sans .gz
        fname = _fp.name
        return fname[:-3] if fname.endswith('.gzip') else fname

    if flag & gzip.FEXTRA:
        # Read & discard the extra field, if present
        extra_len, = struct.unpack("<H", _read_exact(_fp, 2))
        _read_exact(_fp, extra_len)

    _fname = []  # bytes for fname
    if flag & gzip.FNAME:
        # Read a null-terminated string containing the filename
        # RFC 1952 <https://tools.ietf.org/html/rfc1952>
        #    specifies FNAME is encoded in latin1
        while True:
            s = _fp.read(1)
            if not s or s == b'\000':
                break
            _fname.append(s)
        # The FNAME field is user-supplied data. Sanitize it before returning
        # so callers cannot use it to build a path that escapes the intended
        # location (CWE-73 path manipulation).
        return _sanitize_gz_inner_filename(
            ''.join([s.decode('latin1') for s in _fname]))

    return None

def _read_exact(fp, n):
    """This is the gzip.GzipFile._read_exact() method from the
    Python library.
    """
    data = fp.read(n)
    while len(data) < n:
        b = fp.read(n - len(data))
        if not b:
            raise EOFError("Compressed file ended before the "
                           "end-of-stream marker was reached")
        data += b
    return data
