'''Utility functions'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
import collections
import datetime
import email.utils
import hashlib
import http.client
import io
import logging
import os
import tempfile
import threading
import urllib.parse


_logger = logging.getLogger(__name__)


def printable_str_to_str(s):
    return s.translate(str.maketrans('', '', '\t\r\n'))\
        .replace(r'\r', '\r')\
        .replace(r'\n', '\n')\
        .replace(r'\t', '\t')


def find_file_pattern(file_obj, pattern, bufsize=512, limit=4096,
inclusive=False):
    '''Find the offset from current position of pattern'''

    original_position = file_obj.tell()
    bytes_read = 0
    # FIXME: don't accumulate growing buffer
    search_buf = io.BytesIO()

    while True:
        if limit:
            size = min(bufsize, limit - bytes_read)
        else:
            size = bufsize

        data = file_obj.read(size)

        if not data:
            break

        search_buf.write(data)

        try:
            index = search_buf.getvalue().index(pattern)
        except ValueError:
            pass
        else:
            offset = index

            if inclusive:
                offset += len(pattern)

            file_obj.seek(original_position)
            return offset

        bytes_read += len(data)

    file_obj.seek(original_position)

    raise ValueError('Search for pattern exhausted')


def strip_warc_extension(s):
    '''Removes ``.warc`` or ``.warc.gz`` from filename'''

    if s.endswith('.gz'):
        s = s[:-3]

    if s.endswith('.warc'):
        s = s[:-5]

    return s


class DiskBufferedReader(io.BufferedIOBase):
    '''Buffers the file to disk large parts at a time'''

    # Some segments lifted from _pyio.py
    # Copyright 2001-2011 Python Software Foundation
    # Licensed under Python Software Foundation License Version 2

    def __init__(self, raw, disk_buffer_size=104857600, spool_size=10485760):
        io.BufferedIOBase.__init__(self)
        self._raw = raw
        self._disk_buffer_size = disk_buffer_size
        self._offset = 0
        self._block_index = None
        self._block_file = None
        self._spool_size = spool_size
        self._lock = threading.RLock()
        self._cache = FileCache()

        self._set_block(0)

    def _set_block(self, index):
        if index == self._block_index:
            return

        with self._lock:
            self._block_index = index

            self._block_file = self._cache.get(self._block_index)

            if self._block_file:
                _logger.debug('Buffer block file cache hit. index=%d',
                    self._block_index)
            else:
                _logger.debug('Creating buffer block file. index=%d',
                    self._block_index)

                self._block_file = tempfile.SpooledTemporaryFile(
                    max_size=self._spool_size)

                self._raw.seek(self._block_index * self._disk_buffer_size)
                copyfile_obj(self.raw, self._block_file,
                    max_length=self._disk_buffer_size)
                self._cache.put(self._block_index, self._block_file)

                _logger.debug('Buffer block file created. length=%d',
                    self._block_file.tell())

            self._block_file.seek(0)

    def tell(self):
        return self._offset

    def seek(self, pos, whence=0):
        if not (0 <= whence <= 1):
            raise ValueError('Bad whence argument')

        with self._lock:
            if whence == 1:
                self._offset += pos

            self._offset = pos
            index = self._offset // self._disk_buffer_size
            self._set_block(index)
            self._block_file.seek(self._offset % self._disk_buffer_size)

    def read(self, n=None):
        buf = io.BytesIO()
        bytes_left = n

        with self._lock:
            while True:
                self.seek(self._offset)
                data = self._block_file.read(bytes_left)
                self._offset += len(data)
                buf.write(data)
                bytes_left -= len(data)

                if not data:
                    break

                if bytes_left <= 0:
                    break

        return buf.getvalue()

    def peek(self, n=0):
        with self._lock:
            original_position = self.tell()
            data = self.read(n)
            self.seek(original_position)
        return data

    def seekable(self):
        return self.raw.seekable()

    def readable(self):
        return self.raw.readable()

    def writable(self):
        return False

    @property
    def raw(self):
        return self._raw

    @property
    def closed(self):
        return self.raw.closed

    @property
    def name(self):
        return self.raw.name

    @property
    def mode(self):
        return self.raw.mode

    def fileno(self):
        return self.raw.fileno()

    def isatty(self):
        return self.raw.isatty()


class FileCache(object):
    '''A cache containing references to file objects.

    File objects are closed when expired. Class is thread safe and will
    only return file objects belonging to its own thread.
    '''

    def __init__(self, size=4):
        self._size = size
        self._files = collections.deque()
        self._lock = threading.Lock()

    def get(self, filename):
        thread_id = threading.current_thread()

        with self._lock:
            return self._get(filename, thread_id)

    def _get(self, filename, thread_id):
        for cache_filename, cache_thread_id, file_obj in self._files:
            if filename == cache_filename and thread_id == cache_thread_id:
                return file_obj

    def put(self, filename, file_obj):
        thread_id = threading.current_thread()

        with self._lock:
            if self._get(filename, thread_id):
                return

            if len(self._files) > self._size:
                old_file_obj = self._files.popleft()[2]
                old_file_obj.close()

            self._files.append((filename, thread_id, file_obj))


def copyfile_obj(source, dest, bufsize=4096, max_length=None,
write_attr_name='write'):
    '''Like :func:`shutil.copyfileobj` but with limit on how much to copy'''

    bytes_read = 0
    write_func = getattr(dest, write_attr_name)

    while True:
        if max_length != None:
            read_size = min(bufsize, max_length - bytes_read)
        else:
            read_size = bufsize

        data = source.read(read_size)

        if not data:
            break

        write_func(data)
        bytes_read += len(data)


class HTTPSocketShim(io.BytesIO):
    def makefile(self, *args, **kwargs):
        return self


def parse_http_response(file_obj):
    '''Parse and return :class:`http.client.HTTPResponse`'''

    response = http.client.HTTPResponse(HTTPSocketShim(file_obj))
    response.begin()

    return response


def split_url_to_filename(s):
    '''Attempt to split a URL to a filename on disk'''

    url_info = urllib.parse.urlsplit(s)

    l = [sanitize_str(url_info.netloc)]

    for part in url_info.path.lstrip('/').split('/'):
        part = sanitize_str(part)

        l.append(part)

        if not part:
            l[-1] = append_index_filename(part)

    if url_info.query:
        l[-1] += '_' + sanitize_str(url_info.query)

    if frozenset([os.curdir, os.pardir, '.', '..']) & frozenset(l):
        raise ValueError('Path contains directory traversal filenames')

    return l


SANITIZE_BLACKLIST = frozenset(
    r'/\:*?"<>|' + ''.join([chr(i) for i in range(0, 32)]) + '\x7f'
)


def sanitize_str(s):
    '''Replaces unsavory chracters from string with an underscore'''

    return ''.join([c if c not in SANITIZE_BLACKLIST else '_' for c in s])


def append_index_filename(path):
    '''Adds ``_index_xxxxxx`` to the path.

    It uses the basename aka filename of the path to generate the hex hash
    digest suffix.
    '''

    hasher = hashlib.sha1(os.path.basename(path).encode())
    path += '_index_{}'.format(hasher.hexdigest()[:6])

    return path


def rename_filename_dirs(dest_filename):
    '''Renames files if they conflict with a directory in given path.

    If a file has the same name as the directory, the file is renamed
    using :func:`append_index_filename`.
    '''

    path = dest_filename
    while True:
        path, filename = os.path.split(path)

        if not filename:
            break

        if os.path.isfile(path):
            new_path = append_index_filename(path)

            _logger.debug('Rename %s -> %s', path, new_path)
            os.rename(path, new_path)
            break


def parse_http_date(s):
    t = email.utils.parsedate_tz(s)

    if not t:
        raise ValueError('Unable to parse date')

    tzinfo = datetime.timezone(datetime.timedelta(seconds=t[9]))
    d = datetime.datetime(*t[:6], tzinfo=tzinfo)

    return d


file_cache = FileCache()
'''The :class:`FileCache` instance'''
