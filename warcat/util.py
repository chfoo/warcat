'''Utility functions'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
import collections
import io
import logging
import shutil
import tempfile
import threading


_logger = logging.getLogger(__name__)


def printable_str_to_str(s):
    return s.translate(str.maketrans('', '', '\t\r\n'))\
        .replace(r'\r', '\r')\
        .replace(r'\n', '\n')\
        .replace(r'\t', '\t')


def find_file_pattern(file_obj, pattern, bufsize=512, limit=None,
inclusive=False):
    original_position = file_obj.tell()
    bytes_read = 0

    while True:
        if limit:
            size = min(bufsize, limit - bytes_read)
        else:
            size = bufsize

        data = file_obj.read(size)

        if not data:
            break

        try:
            index = data.index(pattern)
        except ValueError:
            pass
        else:
            offset = bytes_read + index

            if inclusive:
                offset += len(pattern)

            file_obj.seek(original_position)
            return offset

        bytes_read += len(data)

    file_obj.seek(original_position)
    return bytes_read


def strip_warc_extension(s):
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

        self._set_block(0)

    def _set_block(self, index):
        if index == self._block_index:
            return

        with self._lock:
            self._block_index = index

            _logger.debug('Creating buffer block file. index=%d',
                self._block_index)

            self._block_file = tempfile.SpooledTemporaryFile(
                max_size=self._spool_size)

            self._raw.seek(self._block_index * self._disk_buffer_size)
            copyfile_obj(self.raw, self._block_file,
                max_length=self._disk_buffer_size)

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
    def __init__(self, size=4):
        self._size = size
        self._files = collections.deque()

    def get(self, filename):
        for cache_filename, file_obj in self._files:
            if filename == cache_filename:
                return file_obj

    def put(self, filename, file_obj):
        for cache_filename, file_obj in self._files:
            if filename == cache_filename:
                return

        if len(self._files) > self._size:
            old_file_obj = self._files.popleft()[1]
            old_file_obj.close()

        self._files.append((filename, file_obj))


def copyfile_obj(source, dest, bufsize=4096, max_length=None):
    bytes_read = 0

    while True:
        if max_length != None:
            read_size = min(bufsize, max_length - bytes_read)
        else:
            read_size = bufsize

        data = source.read(read_size)

        if not data:
            break

        dest.write(data)
        bytes_read += len(data)


file_cache = FileCache()
