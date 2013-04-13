'''Content blocks and payload blocks'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
from warcat import util
from warcat.model.binary import BytesSerializable, BinaryFileRef
from warcat.model.common import FIELD_DELIM_BYTES, NEWLINE_BYTES
from warcat.model.field import HTTPHeader, Fields
import logging


_logger = logging.getLogger(__name__)


class ContentBlock(BytesSerializable):
    @classmethod
    def load(cls, file_obj, length, content_type):
        '''Load and return :class:`BinaryBlock` or :class:`BlockWithPayload`'''

        if content_type.startswith('application/http'):
            return BlockWithPayload.load(file_obj, length,
                field_cls=HTTPHeader)
        elif content_type.startswith('application/warc-fields'):
            return BlockWithPayload.load(file_obj, length, field_cls=Fields)
        else:
            return BinaryBlock.load(file_obj, length)


class BinaryBlock(ContentBlock, BinaryFileRef):
    '''A content block that is octet data'''

    def iter_bytes(self):
        for v in self.iter_file():
            yield v

    @classmethod
    def load(cls, file_obj, length):
        '''Return a :class:`BinaryBlock` using given file object'''

        binary_block = BinaryBlock()
        binary_block.set_file(file_obj.name, offset=file_obj.tell(),
            length=length)

        file_obj.seek(file_obj.tell() + length)

        _logger.debug('Binary content block length=%d', binary_block.length)

        return binary_block


class BlockWithPayload(ContentBlock):
    '''A content block (fields/data) within a :class:`Record`.

    .. attribute:: fields

        :class:`Fields`

    .. attribute:: payload

        :class:`Payload`

    .. attribute:: binary_block

        If this block was loaded from a file, this attribute will be a
        :class:`BinaryBlock` of the original file. Otherwise, this attribute
        is `None`.
    '''

    def __init__(self, fields=None, payload=None):
        self.fields = fields or Fields()
        self.payload = payload or Payload()
        self.binary_block = None

    @classmethod
    def load(cls, file_obj, length, field_cls):
        '''Return a :class:`BlockWithPayload`

        :param file_obj: The file object
        :param length: How much to read from the file
        :param field_cls: The class or subclass of :class:`Fields`
        '''

        binary_block = BinaryBlock()
        binary_block.set_file(file_obj.name, file_obj.tell(), length)

        try:
            field_length = util.find_file_pattern(file_obj, FIELD_DELIM_BYTES,
                limit=length, inclusive=True)
        except ValueError:
            # No payload
            field_length = length

        fields = field_cls.parse(file_obj.read(field_length).decode())
        payload_length = length - field_length
        payload = Payload()

        payload.set_file(file_obj.name, offset=file_obj.tell(),
            length=payload_length)
        _logger.debug('Field length=%d', field_length)
        _logger.debug('Payload length=%d', payload_length)

        file_obj.seek(file_obj.tell() + payload_length)

        block = BlockWithPayload(fields, payload)
        block.binary_block = binary_block

        return block

    @property
    def length(self):
        '''Return the new computed length'''

        return (len(bytes(self.fields)) + len(NEWLINE_BYTES) +
            self.payload.length)

    def iter_bytes(self):
        for v in self.fields.iter_bytes():
            yield v

        yield NEWLINE_BYTES

        for v in self.payload.iter_bytes():
            yield v


class Payload(BytesSerializable, BinaryFileRef):
    '''Data within a content block that has fields'''

    def __init__(self):
        BinaryFileRef.__init__(self)

    def iter_bytes(self):
        for v in self.iter_file():
            yield v


__all__ = ['ContentBlock', 'BinaryBlock', 'BlockWithPayload', 'Payload']
