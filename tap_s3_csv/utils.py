import gzip
import os
import struct


def sanitize_gz_file_name(file_name):
    """Sanitize the original filename embedded in a gzip header before it is
    used to build a downstream file path.

    The embedded name is user-supplied (it comes from the uploaded file's gzip
    header), so it must never be allowed to traverse directories or escape the
    expected location (CWE-73 / path manipulation). We normalize path
    separators and keep only the final path component, and reject any residual
    parent-directory references.
    """
    if not file_name:
        return file_name

    # Normalize Windows separators, then keep only the final path component so
    # that any embedded directory traversal (e.g. "../../etc/passwd") is
    # stripped down to a bare filename.
    normalized = file_name.replace("\\", "/")
    base_name = os.path.basename(normalized)

    # basename of names like ".." or "foo/" can still be empty or a traversal
    # token; treat those as invalid so callers skip the file.
    if base_name in ("", ".", ".."):
        return ""

    return base_name


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
        return ''.join([s.decode('latin1') for s in _fname])

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
