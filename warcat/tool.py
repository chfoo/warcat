from warcat import util
from warcat.model import WARC
import abc
import gzip
import isodate
import logging
import os.path
import sys


_logger = logging.getLogger(__name__)


class BaseIterateTool(metaclass=abc.ABCMeta):
    def __init__(self, filenames, out_file=sys.stdout.buffer, write_gzip=False,
    force_read_gzip=None, read_record_ids=None, preserve_block=True):
        self.filenames = filenames
        self.out_file = out_file
        self.force_read_gzip = force_read_gzip
        self.write_gzip = write_gzip
        self.current_filename = None
        self.read_record_ids = read_record_ids
        self.preserve_block = preserve_block

        self.init()

    def init(self):
        pass

    def preprocess(self):
        pass

    def postprocess(self):
        pass

    def process(self):
        self.num_records = 0

        for filename in self.filenames:
            self.record_order = 0
            self.current_filename = filename

            f = WARC.open(filename, force_gzip=self.force_read_gzip)

            while True:
                record, has_more = WARC.read_record(f,
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
                    break

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
