'''Named fields'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
from warcat.model.binary import StrSerializable, BytesSerializable
from warcat.model.common import NEWLINE
import collections
import logging
import re


_logger = logging.getLogger(__name__)


class Fields(StrSerializable, BytesSerializable):
    '''Name and value pseudo-map list

    Behaves like a `dict` or mutable mapping. Mutable mapping operations
    remove any duplicates in the field list.
    '''

    def __init__(self, field_list=None):
        self._list = [] if field_list is None else field_list

    def __contains__(self, name):
        return self.get(name) is not None

    def __iter__(self):
        return self._list

    def __len__(self):
        return len(self._list)

    def __getitem__(self, name):
        for k, v in self._list:
            if k.lower() == name.lower():
                return v

        raise KeyError('{} not in fields'.format(name))

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
        '''Append a name-value field to the list'''
        self._list.append((name, value))

    def get(self, name, default=None):
        try:
            return self[name]
        except KeyError:
            return default

    def get_list(self, name):
        '''Return a list of values'''

        return list([x for x in self._list if x[0].lower() == name.lower()])

    def count(self, name):
        '''Count the number of times this name occurs in the list'''

        return len(self.get_list(name))

    def index(self, name):
        '''Return the index of the first occurance of given name'''

        for i in range(len(self._list)):
            if self._list[i][0].lower() == name.lower():
                return i

        raise KeyError('Name {} not found in fields'.format(name))

    def list(self):
        '''Return the underlying list'''

        return self._list

    def keys(self):
        return [x[0] for x in self._list]

    def values(self):
        return [x[1] for x in self._list]

    def clear(self):
        self._list[:] = []

    def iter_str(self):
        for name, value in self._list:
            if value:
                yield '{}: {}'.format(name, value)
            else:
                yield '{}:'.format(name)
            yield NEWLINE

    def iter_bytes(self):
        for s in self.iter_str():
            yield s.encode()

    @classmethod
    def parse(cls, s, newline=NEWLINE):
        '''Parse a named field string and return a :class:`Fields`'''

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
        '''Scan for multiline value which is prefixed with a space or tab'''

        while len(lines):
            line = lines.popleft()

            if line == '':
                break
            if line[0] not in (' ', '\t'):
                lines.appendleft(line)
                break

            value = '{}{}'.format(value, line[1:])

        return value


class Header(StrSerializable, BytesSerializable):
    '''A header of a WARC Record.

    .. attribute:: version

        A `str` containing the version

    .. attribute:: fields

        The :class:`Fields` object.
    '''

    VERSION = '1.0'

    def __init__(self, version=VERSION, fields=None):
        self.version = version
        self.fields = fields or Fields()

    @classmethod
    def parse(cls, b):
        '''Parse from `bytes` and return :class:`Header`'''

        version_line, field_str = b.decode().split(NEWLINE, 1)

        _logger.debug('Version line=%s', version_line)

        if not version_line.startswith('WARC'):
            raise IOError('Wrong WARC header')

        header = Header(version_line[5:], Fields.parse(field_str))

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


class HTTPHeader(Fields):
    '''Fields extended with a HTTP status attribute.

    .. attribute:: status

        The `str` of the HTTP status message and code.
    '''

    def __init__(self, field_list=None, status=None):
        Fields.__init__(self, field_list=field_list)
        self.status = status

    @property
    def status_code(self):
        return int(self.status.split()[1])

    @classmethod
    def parse(cls, s, newline=NEWLINE):
        http_headers = HTTPHeader()
        http_headers.status, s = s.split(newline, 1)

        fields = super(HTTPHeader, cls).parse(s, newline=newline)
        http_headers.list().extend(fields.list())

        return http_headers

    def iter_str(self):
        yield self.status
        yield NEWLINE

        for s in Fields.iter_str(self):
            yield s


HTTPHeaders = HTTPHeader
'''.. deprecated:: 2.1.1

    Name uses wrong inflection. Use :class:`HTTPHeader` instead.
'''

__all__ = ['Fields', 'HTTPHeader', 'HTTPHeaders', 'Header']
