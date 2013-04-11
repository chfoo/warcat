'''Archive process tools'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
from warcat import model, util
import abc
import gzip
import http.client
import isodate
import logging
import os.path
import sys
import shutil
import itertools


_logger = logging.getLogger(__name__)


THROBBER = [
    '(=   |',
    '|=   |',
    '| =  |',
    '|  = |',
    '|   =)',
    '|   =|',
    '|  = |',
    '| =  |',
]
# lol; so bouncy.


class BaseIterateTool(metaclass=abc.ABCMeta):
    '''Base class for iterating through records'''

    def __init__(self, filenames, out_file=sys.stdout.buffer, write_gzip=False,
    force_read_gzip=None, read_record_ids=None, preserve_block=True,
    out_dir=None, print_progress=False):
        self.filenames = filenames
        self.out_file = out_file
        self.force_read_gzip = force_read_gzip
        self.write_gzip = write_gzip
        self.current_filename = None
        self.read_record_ids = read_record_ids
        self.preserve_block = preserve_block
        self.out_dir = out_dir
        self.print_progress = print_progress

        self.init()

    def init(self):
        pass

    def preprocess(self):
        pass

    def postprocess(self):
        pass

    def process(self):
        self.num_records = 0
        throbber_iter = itertools.cycle(THROBBER)
        progress_msg = ''

        for filename in self.filenames:
            self.record_order = 0
            self.current_filename = filename

            f = model.WARC.open(filename, force_gzip=self.force_read_gzip)

            while True:
                record, has_more = model.WARC.read_record(f,
                    preserve_block=self.preserve_block)

                skip = False

                if self.read_record_ids:
                    if record.record_id not in self.read_record_ids:
                        skip = True

                if skip:
                    _logger.debug('Skipping %s due to filter',
                        record.record_id)
                else:
                    self.action(record)

                if not has_more:
                    if self.print_progress:
                        sys.stderr.write('\n')

                    break

                if self.print_progress and self.num_records % 100 == 0:
                    s = next(throbber_iter)
                    sys.stderr.write('\b' * len(progress_msg))
                    progress_msg = '{} {} '.format(self.num_records, s)
                    sys.stderr.write(progress_msg)
                    sys.stderr.flush()

                self.record_order += 1
                self.num_records += 1

            f.close()

    @abc.abstractmethod
    def action(self, record):
        pass


class ListTool(BaseIterateTool):
    def action(self, record):
        print('Record:', record.record_id)
        print('  Order:', self.num_records)
        print('  File offset:', record.file_offset)
        print('  Type:', record.warc_type)
        print('  Date:', isodate.datetime_isoformat(record.date))
        print('  Size:', record.content_length)


class ConcatTool(BaseIterateTool):
    def init(self):
        self.bytes_written = 0

    def action(self, record):
        if self.write_gzip:
            f = gzip.GzipFile(fileobj=self.out_file, mode='wb')
        else:
            f = self.out_file

        for v in record.iter_bytes():
            _logger.debug('Wrote %d bytes', len(v))
            f.write(v)
            self.bytes_written += len(v)

        if self.write_gzip:
            f.close()

        if self.num_records % 1000 == 0:
            _logger.info('Wrote %d records (%d bytes) so far',
                self.num_records, self.bytes_written)


class SplitTool(BaseIterateTool):
    def action(self, record):
        record_filename = '{}.{:08d}.warc'.format(
            util.strip_warc_extension(os.path.basename(self.current_filename)),
            self.record_order)
        record_filename = os.path.join(self.out_dir, record_filename)

        os.makedirs(self.out_dir, exist_ok=True)

        if self.write_gzip:
            record_filename += '.gz'
            f = gzip.GzipFile(record_filename, mode='wb')
        else:
            f = open(record_filename, 'wb')

        for v in record.iter_bytes():
            _logger.debug('Wrote %d bytes', len(v))
            f.write(v)

        f.close()

        if self.num_records % 1000 == 0:
            _logger.info('Wrote %d records so far', self.num_records)


class ExtractTool(BaseIterateTool):
    def action(self, record):
        if record.warc_type != 'response':
            return
        if not isinstance(record.content_block, model.BlockWithPayload):
            return
        if not isinstance(record.content_block.fields, model.HTTPHeaders):
            return
        if not record.content_block.fields.status_code == http.client.OK:
            return

        url = record.header.fields['WARC-Target-URI']
        binary_block = record.content_block.binary_block
        file_obj = binary_block.get_file_object()
        data = file_obj.read(binary_block.length)
        response = util.parse_http_response(data)
        path_list = util.split_url_to_filename(url)
        path = os.path.join(self.out_dir, *path_list)
        dir_path = os.path.dirname(path)

        if os.path.isdir(path):
            path = util.append_index_filename(path)

        _logger.debug('Extracting %s to %s', record.record_id, path)
        util.rename_filename_dirs(path)
        os.makedirs(dir_path, exist_ok=True)

        with open(path, 'wb') as f:
            shutil.copyfileobj(response, f)

        # TODO: set modified time to last modified
        _logger.info('Extracted %s to %s', record.record_id, path)
