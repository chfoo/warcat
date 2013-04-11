from warcat import model, util
import io
import os.path
import unittest


fields_str = util.printable_str_to_str(
r'''WARC-Type: response\r\n
Non-ASCII:    ðëARCHIVE#:>Þ   \r\n
Multiline: The quick brown fox\r\n
 jumps\r\n
\tover\n
   the lazy dog.\r\n
Content-LENGTH: 10\r\n
''')


class TestModel(unittest.TestCase):
    def test_fields_parse(self):
        fields = model.Fields.parse(fields_str)

        self.assertEqual('response', fields['warc-type'])
        self.assertEqual('ðëARCHIVE#:>Þ   ', fields['non-ascii'])
        self.assertEqual(
            'The quick brown foxjumpsover\n   the lazy dog.',
            fields['multiline'])
        self.assertEqual('10', fields['content-length'])

    def test_fields(self):
        fields = model.Fields()
        fields.add('My-Name', 'a')
        fields['Animal'] = 'kitten'
        fields.add('my-name', 'b')

        self.assertListEqual(
            [('My-Name', 'a'), ('Animal', 'kitten'), ('my-name', 'b')],
            fields.list())
        self.assertIn('my-name', fields)
        self.assertNotIn('content-length', fields)
        self.assertEqual('a', fields['my-name'])
        self.assertEqual(2, fields.count('my-name'))

        fields['my-name'] = 'c'

        self.assertEqual(1, fields.count('my-name'))
        self.assertEqual('kitten', fields['animal'])

    def test_build_model(self):
        warc = model.WARC()

        record = model.Record()
        record.content_type = 'text/plain'
        record.record_id = 'a'
        record.content_block = model.BinaryBlock()
        content_data = b'hello world!'
        record.content_block.set_file(io.BytesIO(content_data))
        record.content_length = len(content_data)
        warc.records.append(record)

        record = model.Record()
        record.content_type = 'application/warc-fields'
        record.record_id = 'b'
        record.content_block = model.BlockWithPayload()
        record.content_block.fields['example'] = 'kitten'
        content_data = b'hello world!'
        record.content_block.payload.set_file(io.BytesIO(content_data),
            length=len(content_data))
        record.content_length = record.content_block.length
        warc.records.append(record)

        bytes(warc)


class TestReading(unittest.TestCase):
    test_dir = os.path.join('example')

    def test_read_c1_warcinfo(self):
        warc = model.WARC()

        warc.load(os.path.join(self.test_dir, 'c1_warcinfo.warc'))
        self.assertEqual(1, len(warc.records))

        record = warc.records[0]

        self.assertEqual('0.18', record.header.version)
        self.assertEqual('warcinfo', record.warc_type)
        self.assertEqual(400, record.content_length,
            'newline between named fields and 0-length payload not accounted'
            ' for')
        self.assertEqual(5, len(record.header.fields))

        content_block = record.content_block
        self.assertEqual('Heritrix 1.12.0 http://crawler.archive.org',
            content_block.fields['software'])
        self.assertEqual(
            'http://www.archive.org/documents/WarcFileFormat-0.18.html',
            content_block.fields['conformsTo'])

    def test_read_at_warc(self):
        warc = model.WARC()

        warc.load(os.path.join(self.test_dir, 'at.warc'))
        self.assertEqual(8, len(warc.records))

    def test_read_at_warc_gzip(self):
        warc = model.WARC()

        warc.load(os.path.join(self.test_dir, 'at.warc.gz'))
        self.assertEqual(8, len(warc.records))

    def test_warc_to_bytes(self):
        warc = model.WARC()

        warc.load(os.path.join(self.test_dir, 'at.warc'))
        bytes(warc)
