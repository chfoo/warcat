# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
from warcat import util
from warcat.model import WARC
import argparse
import gzip
import isodate
import logging
import os.path
import sys
import warcat.version

_logger = logging.getLogger(__name__)


def main():
    arg_parser = argparse.ArgumentParser(version=warcat.version.__version__,
        description='Tool for handling Web ARChive (WARC) files.')
    arg_parser.add_argument('command',
        help='A command to run. Use "help" for a list.')
    arg_parser.add_argument('file', help='Filename of file to be read.',
        nargs='*')
    arg_parser.add_argument('--output', '-o', metavar='FILE',
        help='Output to FILE instead of standard out',
        type=argparse.FileType('wb'), default=sys.stdout,
    )
    arg_parser.add_argument('--gzip', '-z', action='store_true',
        help='When outputing a file, use gzip compression',
    )
    arg_parser.add_argument('--force-read-gzip', action='store_true',
        help='Instead of guessing by filename, force reading archives as'
        ' gzip compressed')
    arg_parser.add_argument('--verbose', action='count')

    original_print_help = arg_parser.print_help

    def help_monkeypatch(file=None):
        original_print_help(file)
        print(file=file)
        help_command(file=file)

    arg_parser.print_help = help_monkeypatch

    args = arg_parser.parse_args()

    if args.verbose:
        if args.verbose > 1:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

    command_info = commands.get(args.command)

    if command_info:
        command_info[1](args)
    else:
        help_command(args)


def help_command(args=None, file=sys.stderr):
    print('Commands:', file=file)

    for command in sorted(commands):
        label = commands[command][0]
        print('{}\n    {}'.format(command, label), file=file)


def list_command(args):
    for filename in args.file:
        f = WARC.open(filename, force_gzip=args.force_read_gzip)

        while True:
            record, has_more = WARC.read_record(f)

            print('Record:', record.record_id)
            print('  File offset:', record.file_offset)
            print('  Type:', record.warc_type)
            print('  Date:', isodate.datetime_isoformat(record.date))
            print('  Size:', record.content_length)

            if not has_more:
                break

        f.close()


def pass_command(args):
    out_file = args.output

    if out_file == sys.stdout:
        out_file = sys.stdout.buffer

    for filename in args.file:
        warc = WARC()
        warc.load(filename, force_gzip=args.force_read_gzip)

        for v in warc.iter_bytes():
            out_file.write(v)


def concat_command(args):
    if args.output == sys.stdout:
        file_obj = sys.stdout.buffer
    else:
        file_obj = args.output
        file_obj.truncate()

    bytes_written = 0
    records_written = 0
    for filename in args.file:
        source_f = WARC.open(filename, force_gzip=args.force_read_gzip)

        while True:
            record, has_more = WARC.read_record(source_f)

            if args.gzip:
                f = gzip.GzipFile(fileobj=file_obj, mode='wb')
            else:
                f = file_obj
            for v in record.iter_bytes():
                _logger.debug('Wrote %d bytes', len(v))
                f.write(v)
                bytes_written += len(v)

            if args.gzip:
                f.close()

            records_written += 1

            if records_written % 1000 == 0:
                _logger.info('Wrote %d records (%d bytes) so far',
                    records_written, bytes_written)

            if not has_more:
                break

        source_f.close()


def split_command(args):
    for filename in args.file:
        source_f = WARC.open(filename, force_gzip=args.force_read_gzip)
        i = 0

        while True:
            record, has_more = WARC.read_record(source_f)

            record_filename = '{}.{:08d}.warc'.format(
                util.strip_warc_extension(os.path.basename(filename)), i)

            if args.gzip:
                record_filename += '.gz'
                f = gzip.GzipFile(record_filename, mode='wb')
            else:
                f = open(record_filename, 'wb')

            for v in record.iter_bytes():
                _logger.debug('Wrote %d bytes', len(v))
                f.write(v)

            f.close()
            i += 1

            if i % 1000 == 0:
                _logger.info('Wrote %d records so far', i)

            if not has_more:
                break


commands = {
    'help': ('List commands available', help_command),
    'list': ('List contents of archive', list_command),
    'pass': ('Load archive and write it back out', pass_command),
    'concat': ('Naively join archives into one', concat_command),
    'split': ('Split archives into individual records', split_command),
}


if __name__ == '__main__':
    main()
