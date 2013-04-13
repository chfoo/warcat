'''WARC model starting point'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
from warcat import util
from warcat.model.binary import BytesSerializable
from warcat.model.common import FIELD_DELIM_BYTES
from warcat.model.record import Record
import gzip
import logging


_logger = logging.getLogger(__name__)


class WARC(BytesSerializable):
    '''A Web ARChive file model.

    Typically, large streaming operations should use :func:`open` and
    :func:`read_record` functions.
    '''

    def __init__(self):
        self.records = []

    def load(self, filename):
        '''Open and load the contents of the given filename.

        The records are located in :attr:`records`.
        '''

        f = self.open(filename)
        self.read_file_object(f)
        f.close()

    def read_file_object(self, file_object):
        '''Read records until the file object is exhausted'''

        while True:
            record, has_more = self.read_record(file_object)
            self.records.append(record)
            if not has_more:
                break

    @classmethod
    def open(cls, filename, force_gzip=False):
        '''Return a logical file object.

        :param filename: The path of the file. gzip compression is detected
            using file extension.
        :param force_gzip: Use gzip compression always.
        '''

        if filename.endswith('.gz') or force_gzip:
            f = gzip.open(filename)
            _logger.info('Opened gziped file %s', filename)
            return util.DiskBufferedReader(f)
        else:
            f = open(filename, 'rb')
            _logger.info('Opened file %s', filename)
            return f

    @classmethod
    def read_record(cls, file_object, preserve_block=False):
        '''Return a record and whether there are more records to read.

        .. seealso:: :class:`Record`

        :return: A tuple. The first item is the :class:`Record`. The second
            item is a boolean indicating whether there are more records to
            be read.
        '''

        record = Record.load(file_object, preserve_block=preserve_block)
        _logger.debug('Finished reading a record %s', record.record_id)

        data = file_object.read(len(FIELD_DELIM_BYTES))

        if data != FIELD_DELIM_BYTES:
            _logger.debug('Wrong delim %s', data)
            raise IOError('Blocks not separated correctly (tell={})'.format(
                file_object.tell()))

        if not file_object.peek(1):
            _logger.info('Finished reading Warc')
            return (record, False)
        else:
            return (record, True)

    def iter_bytes(self):
        for record in self.records:
            for v in record.iter_bytes():
                yield v


__all__ = ['WARC']
