'''Model serialization and binary references'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
from warcat import util
import abc
import gzip
import logging
import tempfile


_logger = logging.getLogger(__name__)


class BytesSerializable(metaclass=abc.ABCMeta):
    '''Metaclass that indicates this object can be serialized to bytes'''

    @abc.abstractmethod
    def iter_bytes(self):
        '''Return an iterable of bytes'''
        pass

    def __bytes__(self):
        return b''.join(self.iter_bytes())


class StrSerializable(metaclass=abc.ABCMeta):
    '''Metaclass that indicates this object can be serialized to str'''

    @abc.abstractmethod
    def iter_str(self):
        '''Return an iterable of str'''
        pass

    def __str__(self):
        return ''.join(self.iter_str())


class BinaryFileRef(metaclass=abc.ABCMeta):
    '''Reference to a file containing the content block data.

    .. attribute:: file_offset

        When reading, the file is seeked to `file_offset`.

    .. attribute:: length

        The length of the data

    .. attribute:: filename

        The filename of the referenced data. It must be a valid file.

    .. attribute:: file_obj

        The file object to be read from. It is important that this file
        object is not shared or race conditions will occur. File objects
        are not closed automatically.

    .. note::

        Either :attr:`filename` or :attr:`file_obj` must be set.
    '''

    def __init__(self):
        self.file_offset = 0
        self.length = None
        self.filename = None
        self.file_obj = None

    def set_file(self, file, offset=0, length=None):
        '''Set the reference to the file or filename with the data.

        This is a convenience function to setting the attributes individually.
        '''

        if hasattr(file, 'read'):
            self.file_obj = file
        else:
            self.filename = file

        self.file_offset = offset
        self.length = length

    def iter_file(self, buffer_size=4096):
        '''Return an iterable of bytes of the source data'''

        with self.get_file(safe=True) as file_obj:
            bytes_read = 0

            while True:
                if self.length is not None:
                    length = min(buffer_size, self.length - bytes_read)
                else:
                    length = buffer_size

                data = file_obj.read(length)
                bytes_read += len(data)

                if not data or not length:
                    break

                yield data

    def get_file(self, safe=True, spool_size=10485760):
        '''Return a file object with the data.

        :param safe:
            If `True`, return a new file object that is a copy of the data.
            You will be responsible for closing the file.

            Otherwise, it will be the original file object that is seeked
            to the correct offset. Be sure to not read beyond its length and
            seek back to the original position if necessary.
        '''

        if self.filename:
            file_obj = util.file_cache.get(self.filename)

            if not file_obj:
                if self.filename.endswith('.gz'):
                    file_obj = util.DiskBufferedReader(
                        gzip.GzipFile(self.filename))
                else:
                    file_obj = open(self.filename, 'rb')

                util.file_cache.put(self.filename, file_obj)
        else:
            file_obj = self.file_obj

        original_position = file_obj.tell()

        if self.file_offset:
            file_obj.seek(self.file_offset)

        if safe:
            _logger.debug('Creating safe file of %s',
                self.filename or self.file_obj)
            temp_file_obj = tempfile.SpooledTemporaryFile(max_size=spool_size)

            util.copyfile_obj(file_obj, temp_file_obj, max_length=self.length)
            temp_file_obj.seek(0)
            file_obj.seek(original_position)

            return temp_file_obj

        return file_obj


__all__ = ['BytesSerializable', 'StrSerializable', 'BinaryFileRef']
