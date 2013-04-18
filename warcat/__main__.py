# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
from warcat.model import WARC
from warcat.tool import ListTool, ConcatTool, SplitTool, ExtractTool, VerifyTool
import argparse
import logging
import os
import sys
import warcat.version

_logger = logging.getLogger(__name__)


def main():
    arg_parser = argparse.ArgumentParser(
        description='Tool for handling Web ARChive (WARC) files.')
    arg_parser.add_argument('--version', action='version',
        version=warcat.version.__version__)
    arg_parser.add_argument('command',
        help='A command to run. Use "help" for a list.')
    arg_parser.add_argument('file', help='Filename of file to be read.',
        nargs='*')
    arg_parser.add_argument('--output', '-o', metavar='FILE',
        help='Output to FILE instead of standard out',
        type=argparse.FileType('wb'), default=sys.stdout,
    )
    arg_parser.add_argument('--gzip', '-z', action='store_true',
        help='When outputting a file, use gzip compression',
    )
    arg_parser.add_argument('--force-read-gzip', action='store_true',
        help='Instead of guessing by filename, force reading archives as'
        ' gzip compressed')
    arg_parser.add_argument('--verbose', action='count',
        help='Increase verbosity. Can be used more than once.')
    arg_parser.add_argument('--record', action='append',
        help='Apply command to record with given ID when reading. '
        'Can be used more than once.')
    arg_parser.add_argument('--preserve-block', action='store_true',
        help="Don't attempt to parse content blocks. Parsed content blocks"
        " may not match content-length and hash digests on serialization.")
    arg_parser.add_argument('--output-dir', '-d',
        default=os.getcwd(),
        help='For output operations that make multiple files, use given'
            ' directory instead of current working directory.')
    arg_parser.add_argument('--progress', action='store_true',
        help='Show progress or activity')
    arg_parser.add_argument('--keep-going', action='store_true',
        help='Continue processing records despite errors')

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


def get_file_buffer(file_obj):
    if file_obj == sys.stdout:
        return sys.stdout.buffer
    else:
        return file_obj


def build_tool(class_, args):
    return class_(args.file,
        write_gzip=args.gzip,
        force_read_gzip=args.force_read_gzip,
        out_file=get_file_buffer(args.output),
        read_record_ids=args.record,
        preserve_block=args.preserve_block,
        out_dir=args.output_dir,
        print_progress=args.progress,
        keep_going=args.keep_going,
    )


def list_command(args):
    tool = build_tool(ListTool, args)
    tool.process()


def pass_command(args):
    out_file = get_file_buffer(args.output)

    for filename in args.file:
        warc = WARC()
        warc.load(filename, force_gzip=args.force_read_gzip)

        for v in warc.iter_bytes():
            out_file.write(v)


def concat_command(args):
    tool = build_tool(ConcatTool, args)
    tool.process()


def split_command(args):
    tool = build_tool(SplitTool, args)
    tool.process()


def extract_command(args):
    tool = build_tool(ExtractTool, args)
    tool.process()


def verify_command(args):
    tool = build_tool(VerifyTool, args)
    tool.process()

    if tool.problems:
        sys.exit('Validation failed. Problems: {}.'.format(tool.problems))


commands = {
    'help': ('List commands available', help_command),
    'list': ('List contents of archive', list_command),
    'pass': ('Load archive and write it back out', pass_command),
    'concat': ('Naively join archives into one', concat_command),
    'split': ('Split archives into individual records', split_command),
    'extract': ('Extract files from archive', extract_command),
    'verify': ('Verify digest and validate conformance', verify_command),
}


if __name__ == '__main__':
    main()
