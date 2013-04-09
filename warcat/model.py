from warcat import util
import abc
import collections
import gzip
import isodate
import logging
import re

NEWLINE = '\r\n'
NEWLINE_BYTES = b'\r\n'
FIELD_DELIM_BYTES = NEWLINE_BYTES * 2
BLOCKS_WITH_FIELDS = ['warcinfo', 'response', 'request', 'metadata', 'revisit']
HTML_BLOCKS = ['response', 'request', 'revisit']

_logger = logging.getLogger(__name__)


class BytesSerializable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def iter_bytes(self):
        pass

    def __bytes__(self):
        return b''.join(self.iter_bytes())


class StrSerializable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def iter_str(self):
        pass

    def __str__(self):
        return ''.join(self.iter_str())


class BinaryFile(metaclass=abc.ABCMeta):
    def __init__(self):
        self.file_offset = None
        self.size = None
        self.filename = None

    def set_source_file(self, filename, offset=0, size=None):
        self.filename = filename
        self.file_offset = offset
        self.size = size

    def iter_bytes_over_source_file(self):
        if hasattr(self.filename, 'name'):
            filename = self.filename.name
        else:
            filename = self.filename

        if filename.endswith('.gz'):
            file_obj = gzip.GzipFile(filename)
        else:
            file_obj = open(filename, 'rb')

        if self.file_offset:
            file_obj.seek(self.file_offset)

        bytes_read = 0

        while True:
            if self.size is not None:
                size = min(4096, self.size - bytes_read)
            else:
                size = 4096

            data = file_obj.read(size)
            bytes_read += len(data)

            if not data or not size:
                break

            yield data

        file_obj.close()


class Fields(StrSerializable, BytesSerializable):
    '''Name and value pseudo-map list'''

    def __init__(self, field_list=None):
        self._list = [] if field_list is None else field_list

    def __contains__(self, name):
        return self[name] is not None

    def __iter__(self):
        return self._list

    def __len__(self):
        return len(self._list)

    def __getitem__(self, name):
        for k, v in self._list:
            if k.lower() == name.lower():
                return v

    def __setitem__(self, name, value):
        try:
            index = self.index(name)
        except KeyError:
            self._list.append((name, value))
        else:
            del self[name]
            self._list.insert(index, (name, value))

    def __delitem__(self, name):
        self._list[:] = [x for x in self._list if x[0].lower() != name.lower()]

    def add(self, name, value):
        self._list.append((name, value))

    def get(self, name, default=None):
        try:
            return self._list['name']
        except KeyError:
            return default

    def get_list(self, name):
        return list([x for x in self._list if x[0].lower() == name.lower()])

    def count(self, name):
        return len(self.get_list(name))

    def index(self, name):
        for i in range(len(self._list)):
            if self._list[i][0].lower() == name.lower():
                return i

        raise KeyError('Name {} not found in fields'.format(name))

    def list(self):
        return self._list

    def keys(self):
        return [x[0] for x in self._list]

    def values(self):
        return [x[1] for x in self._list]

    def clear(self):
        self._list[:] = []

    def iter_str(self):
        for name, value in self._list:
            yield '{}: {}'.format(name, value)
            yield NEWLINE

    def iter_bytes(self):
        for s in self.iter_str():
            yield s.encode()

    @classmethod
    def parse(cls, s, newline=NEWLINE):
        fields = Fields()
        lines = collections.deque(re.split(newline, s))

        while len(lines):
            line = lines.popleft()

            if line == '':
                continue

            name, value = line.split(':', 1)
            value = value.lstrip()
            value = cls.join_multilines(value, lines)
            fields.add(name, value)

        return fields

    @classmethod
    def join_multilines(cls, value, lines):
        while len(lines):
            line = lines.popleft()

            if line == '':
                break
            if line[0] not in (' ', '\t'):
                lines.appendleft(line)
                break

            value = '{}{}'.format(value, line[1:])

        return value


class WARC(BytesSerializable):
    '''A Web ARChive file model'''

    def __init__(self):
        self.records = []

    def load(self, filename):
        if filename.endswith('.gz'):
            with gzip.open(filename) as f:
                _logger.info('Opened gziped file %s', filename)
                self.read_file_object(f)
        else:
            with open(filename, 'rb') as f:
                _logger.info('Opened file %s', filename)
                self.read_file_object(f)

    def read_file_object(self, file_object):
        while True:
            if self.read_record(file_object):
                break

    def read_record(self, file_object):
        record = Record.load(file_object)
        self.records.append(record)
        _logger.info('Read a record %s', record.record_id)

        data = file_object.read(len(FIELD_DELIM_BYTES))

        if data != FIELD_DELIM_BYTES:
            _logger.debug('Wrong delim %s', data)
            raise IOError('Blocks not separated correctly (tell={})'.format(
                file_object.tell()))

        if not file_object.peek(1):
            _logger.info('Finished reading Warc')
            return True

    def iter_bytes(self):
        for record in self.records:
            for v in record.iter_bytes():
                yield v


class Record(BytesSerializable):
    '''A WARC Record within a WARC file'''

    def __init__(self):
        self.header = Header()
        self.content_block = None
        self.file_offset = None

    @classmethod
    def load(cls, file_obj):
        record = Record()
        record.file_offset = file_obj.tell()
        header_size = util.find_file_pattern(file_obj, FIELD_DELIM_BYTES,
            inclusive=True)
        record.header = Header.parse(file_obj.read(header_size))
        block_size = record.content_length

        _logger.debug('Block size=%d', block_size)

        field_cls = None

        if record.warc_type in HTML_BLOCKS:
            field_cls = HTTPHeaders
        elif record.warc_type in BLOCKS_WITH_FIELDS:
            field_cls = Fields

        record.content_block = ContentBlock.load(file_obj, block_size,
            field_cls=field_cls)

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
        for v in self.header.iter_bytes():
            yield v

        if self.content_block:
            for v in self.content_block.iter_bytes():
                yield v

        yield NEWLINE_BYTES
        yield NEWLINE_BYTES


class ContentBlock(BytesSerializable, BinaryFile):
    '''A content block (fields/data) within a Record'''

    def __init__(self):
        BinaryFile.__init__(self)
        self.fields = None
        self.payload = None

    @classmethod
    def load(cls, file_obj, size, field_cls=None):
        content_block = ContentBlock()
        content_block.set_source_file(file_obj, offset=file_obj.tell(),
            size=size)

        if field_cls:
            field_size = util.find_file_pattern(file_obj, FIELD_DELIM_BYTES,
                limit=size, inclusive=True)
            content_block.fields = field_cls.parse(
                file_obj.read(field_size).decode())

            payload_size = size - field_size

            content_block.payload = Payload()
            content_block.payload.set_source_file(file_obj,
                offset=file_obj.tell(), size=payload_size)
            _logger.debug('Field size=%d', field_size)
            _logger.debug('Payload size=%d', payload_size)
        else:
            _logger.debug('Content block size=%d', content_block.size)

        file_obj.seek(content_block.file_offset + content_block.size)
        return content_block

    def iter_bytes(self):
        if self.fields and self.payload:
            for v in self.fields.iter_bytes():
                yield v

            yield NEWLINE_BYTES

            for v in self.payload.iter_bytes():
                yield v
        else:
            for v in self.iter_bytes_over_source_file():
                yield v


class Payload(BytesSerializable, BinaryFile):
    '''Data within a content block'''

    def __init__(self):
        BinaryFile.__init__(self)

    def iter_bytes(self):
        for v in self.iter_bytes_over_source_file():
            yield v


class Header(StrSerializable, BytesSerializable):
    '''A header of a WARC Record'''

    def __init__(self):
        self.version = None
        self.fields = None

    @classmethod
    def parse(cls, b):
        header = Header()
        version_line, field_str = b.decode().split(NEWLINE, 1)

        _logger.debug('Version line=%s', version_line)

        if not version_line.startswith('WARC'):
            raise IOError('Wrong WARC header')

        header.version = version_line[5:]
        header.fields = Fields.parse(field_str)

        return header

    def iter_str(self):
        yield 'WARC/'
        yield self.version
        yield NEWLINE

        for v in self.fields.iter_str():
            yield v

        yield NEWLINE

    def iter_bytes(self):
        for s in self.iter_str():
            yield s.encode()


class HTTPHeaders(Fields):
    '''Fields extended with a HTTP status attribute'''

    def __init__(self, *args):
        Fields.__init__(self, *args)
        self.status = None

    @classmethod
    def parse(cls, s, newline=NEWLINE):
        http_headers = HTTPHeaders()
        http_headers.status, s = s.split(newline, 1)

        fields = super(HTTPHeaders, cls).parse(s, newline=newline)
        http_headers.list().extend(fields.list())

        return http_headers

    def iter_str(self):
        yield self.status
        yield NEWLINE

        for s in Fields.iter_str(self):
            yield s
