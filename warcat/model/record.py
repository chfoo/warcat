'''A WARC record'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
from warcat import util
from warcat.model.binary import BytesSerializable
from warcat.model.block import ContentBlock, BinaryBlock
from warcat.model.common import FIELD_DELIM_BYTES, NEWLINE_BYTES
from warcat.model.field import Header
import isodate
import logging


_logger = logging.getLogger(__name__)


class Record(BytesSerializable):
    '''A WARC Record within a WARC file.


    .. attribute:: header

        :class:`Header`

    .. attribute:: content_block

        A :class:`BinaryBlock` or :class:`BlockWithPayload`

    .. attribute:: file_offset

        If this record was loaded from a file, this attribute contains
        an `int` describing the location of the record in the file.
    '''

    def __init__(self, header=None, content_block=None):
        self.header = header or Header()
        self.content_block = None
        self.file_offset = None

    @classmethod
    def load(cls, file_obj, preserve_block=False):
        '''Parse and return a :class:`Record`

        :param file_object: A file-like object.
        :param preserve_block: If `True`, content blocks are not parsed
            for fields and payloads. Enabling this feature ensures
            preservation of content length and hash digests.
        '''

        _logger.debug('Record start at %d 0x%x', file_obj.tell(),
            file_obj.tell())

        record = Record()
        record.file_offset = file_obj.tell()
        header_length = util.find_file_pattern(file_obj, FIELD_DELIM_BYTES,
            inclusive=True)
        record.header = Header.parse(file_obj.read(header_length))
        block_length = record.content_length

        _logger.debug('Block length=%d', block_length)

        if not preserve_block:
            content_type = record.header.fields.get('content-type')
            record.content_block = ContentBlock.load(file_obj, block_length,
                content_type)
        else:
            record.content_block = BinaryBlock.load(file_obj, block_length)

        new_content_length = record.content_block.length

        if block_length != new_content_length:
            _logger.warn('Content block length changed from %d to %d',
                record.content_length, new_content_length)
            record.content_length = new_content_length

        return record

    @property
    def record_id(self):
        return self.header.fields['WARC-Record-ID']

    @record_id.setter
    def record_id(self, s):
        self.header.fields['WARC-Record-ID'] = s

    @property
    def content_length(self):
        return int(self.header.fields['Content-Length'])

    @content_length.setter
    def content_length(self, i):
        self.header.fields['Content-Length'] = int(i)

    @property
    def date(self):
        return isodate.parse_datetime(self.header.fields['WARC-Date'])

    @date.setter
    def date(self, datetime_obj):
        self.header.fields['WARC-Date'] = isodate.datetime_isoformat(
            datetime_obj)

    @property
    def warc_type(self):
        return self.header.fields['WARC-Type']

    @warc_type.setter
    def warc_type(self, s):
        self.header.fields['WARC-Type'] = s

    def iter_bytes(self):
        _logger.debug('Iter bytes on record %s', self.record_id)

        for v in self.header.iter_bytes():
            yield v

        if self.content_block:
            for v in self.content_block.iter_bytes():
                yield v

        yield NEWLINE_BYTES
        yield NEWLINE_BYTES

__all__ = ['Record']
