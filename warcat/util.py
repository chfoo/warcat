'''Utility functions'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.


def printable_str_to_str(s):
    return s.translate(str.maketrans('', '', '\t\r\n'))\
        .replace(r'\r', '\r')\
        .replace(r'\n', '\n')\
        .replace(r'\t', '\t')


def find_file_pattern(file_obj, pattern, bufsize=1024, limit=None,
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
