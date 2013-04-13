'''Archive process tools'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
from warcat import model, util, verify
import abc
import gzip
import http.client
import isodate
import itertools
import logging
import os.path
import shutil
import sys
import time


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


class VerifyProblem(ValueError):
    def __init__(self, message, iso_section=None, major=True):
        ValueError.__init__(self, message, iso_section, major)

    @property
    def message(self):
        return self.args[0]

    @property
    def iso_section(self):
        return self.args[1]

    @property
    def major(self):
        return self.args[2]


class BaseIterateTool(metaclass=abc.ABCMeta):
    '''Base class for iterating through records'''

    def __init__(self, filenames, out_file=None, write_gzip=False,
    force_read_gzip=None, read_record_ids=None, preserve_block=True,
    out_dir=None, print_progress=False, keep_going=False):
        if not out_file:
            try:
                out_file = sys.stdout.buffer
            except AttributeError:
                out_file = sys.stdout

        self.filenames = filenames
        self.out_file = out_file
        self.force_read_gzip = force_read_gzip
        self.write_gzip = write_gzip
        self.current_filename = None
        self.read_record_ids = read_record_ids
        self.preserve_block = preserve_block
        self.out_dir = out_dir
        self.print_progress = print_progress
        self.keep_going = keep_going

    def preprocess(self):
        pass

    def postprocess(self):
        pass

    def process(self):
        self.num_records = 0
        throbber_iter = itertools.cycle(THROBBER)
        progress_msg = ''
        self.preprocess()

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
                    try:
                        self.action(record)
                    except Exception as e:
                        if self.keep_going:
                            _logger.exception('Error on record %s',
                                record.record_id)
                        else:
                            raise e

                if self.print_progress and self.num_records % 100 == 0:
                    s = next(throbber_iter)
                    sys.stderr.write('\b' * len(progress_msg))
                    progress_msg = '{} {} '.format(self.num_records, s)
                    sys.stderr.write(progress_msg)
                    sys.stderr.flush()

                self.record_order += 1
                self.num_records += 1

                if not has_more:
                    break

            f.close()

        self.postprocess()

        if self.print_progress:
            sys.stderr.write('\nDone. {} records processed.\n'.format(
                self.num_records))

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
    def preprocess(self):
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

        if not os.path.exists(self.out_dir):
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
        file_obj = binary_block.get_file()
        data = file_obj.read(binary_block.length)
        response = util.parse_http_response(data)
        path_list = util.split_url_to_filename(url)
        path = os.path.join(self.out_dir, *path_list)
        dir_path = os.path.dirname(path)

        if os.path.isdir(path):
            path = util.append_index_filename(path)

        # FIXME: long paths such as urls with a long query string may fail

        _logger.debug('Extracting %s to %s', record.record_id, path)
        util.rename_filename_dirs(path)
        os.makedirs(dir_path, exist_ok=True)

        with open(path, 'wb') as f:
            shutil.copyfileobj(response, f)

        last_modified_str = response.getheader('Last-Modified')

        if last_modified_str:
            try:
                last_modified = util.parse_http_date(last_modified_str)
            except ValueError:
                pass
            else:
                timestamp = time.mktime(last_modified.utctimetuple())
                os.utime(path, (time.time(), timestamp))
                _logger.debug('Apply mtime %d to %s', timestamp, path)

        _logger.info('Extracted %s to %s', record.record_id, path)


class VerifyTool(BaseIterateTool):
    MANDATORY_FIELDS = ['WARC-Record-ID', 'Content-Length', 'WARC-Date',
        'WARC-Type']

    def preprocess(self):
        self.record_ids = set()
        self.problems = 0

    def action(self, record):
        verify_actions = [
            self.verify_mandatory_fields,
#            self.check_transfer_encoding,
            self.verify_block_digest,
            self.verify_payload_digest,
            self.verify_id_uniqueness,
            self.verify_id_no_whitespace,
            self.verify_content_type,
            self.verify_concurrent_to,
            self.verify_refers_to,
            self.verify_target_uri,
            self.verify_filename,
            self.verify_profile,
            self.verify_segment_origin_id,
            self.verify_segment_total_length,
        ]

        for action in verify_actions:
            try:
                action(record)
            except VerifyProblem:
                self.problems += 1
                _logger.exception('Record %s failed validation',
                    record.record_id)

    def verify_block_digest(self, record):
        if 'WARC-Block-Digest' in record.header.fields:
            if not verify.verify_block_digest(record):
                raise VerifyProblem('Bad block digest.', '5.8')

            _logger.debug('Block digest ok')

    def verify_payload_digest(self, record):
        if 'WARC-Payload-Digest' in record.header.fields:
            if not verify.verify_payload_digest(record):
                raise VerifyProblem('Bad payload digest.', '5.9')

            _logger.debug('Payload digest ok')

    def verify_id_uniqueness(self, record):
        if record.record_id in self.record_ids:
            raise VerifyProblem('Duplicate record ID.')

        self.record_ids.add(record.record_id)

    def verify_id_no_whitespace(self, record):
        if ' ' in record.record_id:
            raise VerifyProblem('Whitespace in ID', '5.2')

    def check_transfer_encoding(self, record):
        if not isinstance(record.content_block, model.BlockWithPayload):
            return

        if 'Transfer-encoding' in record.content_block.fields:
            raise VerifyProblem('Transfer-encoding found', '5.3.2', False)

    def verify_mandatory_fields(self, record):
        for name in self.MANDATORY_FIELDS:
            if name not in record.header.fields:
                raise VerifyProblem(
                    'Mandatory {} field is missing'.format(name))

    def verify_content_type(self, record):
        if record.warc_type != 'continuation':
            return

        if record.content_length \
        and 'Content-Type' not in record.header.fields:
            raise VerifyProblem('Content-Type should be specified', '5.6',
                False)

    def verify_concurrent_to(self, record):
        if 'WARC-Concurrent-To' not in record.header.fields:
            return

        record_id = record.header.fields['WARC-Concurrent-To']

        if record.warc_type in ('warcinfo', 'conversion', 'continuation'):
            raise VerifyProblem('Unexpected WARC-Concurrent-To', '5.7')

        if record_id not in self.record_ids:
            raise VerifyProblem('Concurrent Record ID {} not seen yet'.format(
                record_id), major=False)

    def verify_refers_to(self, record):
        if 'WARC-Refers-To' not in record.header.fields:
            return

        record_id = record.header.fields['WARC-Refers-To']

        if record.warc_type in ('warcinfo', 'response', 'request',
        'continuation'):
            raise VerifyProblem('WARC-Refers-To field unexpected', '5.11')

        if record_id not in self.record_ids:
            raise VerifyProblem('Refer to record ID {} not seen yet'.format(
                record_id), major=False)

    def verify_target_uri(self, record):
        uri = record.header.fields.get('WARC-Target-URI')

        if not uri and record.warc_type in ('response', 'resource', 'request',
        'revisit', 'conversion', 'continuation'):
            raise VerifyProblem('Expected WARC-Target-URI', '5.12')

        if uri and record.warc_type == 'warc_info':
            raise VerifyProblem('Unexpected WARC-Target-URI', '5.12')

        if uri and ' ' in uri:
            raise VerifyProblem('Whitespace in URI', '5.12')

    def verify_warcinfo_id(self, record):
        record_id = record.header.fields.get('WARC-Warcinfo-ID')

        if record_id and record.warc_type == 'warcinfo':
            raise VerifyProblem('Unexpected WARC-Warcinfo-ID', '5.14')

        if not record_id:
            raise VerifyProblem('Expected WARC-Warcinfo-ID', '5.14', False)

    def verify_filename(self, record):
        if 'WARC-Filename' in record.header.fields \
        and record.warc_type != 'warcinfo':
            raise VerifyProblem('Unexpected WARC-Filename', '5.15')

    def verify_profile(self, record):
        if record.warc_type == 'revisit' \
        and 'WARC-Profile' not in record.header.fields:
            raise VerifyProblem('Expected WARC-Profile', '5.16')

    def verify_segment_origin_id(self, record):
        if record.warc_type == 'continuation':
            if 'WARC-Segment-Origin-ID' not in record.header.fields:
                raise VerifyProblem('Expected WARC-Segment-Origin-ID', '5.19')
        elif 'WARC-Segment-Origin-ID' in record.header.fields:
            raise VerifyProblem('Unexpected WARC-Segment-Origin-ID', '5.19')

    def verify_segment_total_length(self, record):
        if record.warc_type == 'continuation':
            if 'WARC-Segment-Total-Length' not in record.header.fields:
                raise VerifyProblem('Expected WARC-Segment-Total-Length',
                    '5.20')
        elif 'WARC-Segment-Total-Length' in record.header.fields:
            raise VerifyProblem('Unexpected WARC-Segment-Total-Length', '5.20')
