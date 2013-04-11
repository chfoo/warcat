from warcat import util
import datetime
import io
import os.path
import unittest


class TestUtil(unittest.TestCase):
    def test_find_file_pattern(self):
        f = io.BytesIO(b'abcdefg\r\n\r\nhijklmnop')
        offset = util.find_file_pattern(f, b'\r\n\r\n', inclusive=True)
        self.assertEqual(11, offset)
        self.assertEqual(0, f.tell())

    def test_disk_buffered_reader(self):
        test_data = b'0123456789' * 100

        f = util.DiskBufferedReader(io.BytesIO(test_data), disk_buffer_size=42)

        self.assertEqual(b'0', f.peek(1))
        self.assertEqual(b'0', f.read(1))
        self.assertEqual(b'1', f.read(1))
        self.assertEqual(b'2', f.peek(1))

        f.seek(45)
        self.assertEqual(b'5', f.peek(1))
        self.assertEqual(b'56', f.read(2))

        f.seek(41)

        self.assertEqual(b'1234', f.read(4))

        f.seek(0)

        self.assertEqual(b'0', f.peek(1))
        self.assertEqual(b'0', f.read(1))
        self.assertEqual(b'1', f.read(1))
        self.assertEqual(b'2', f.peek(1))

    def test_find_file_pattern_loop_boundary(self):
        for i in range(1000):
            data = b'x' * i + b'\r\n\r\nabcdefghijklmnop'

            f = io.BytesIO(data)

            self.assertEqual(i, util.find_file_pattern(f, b'\r\n\r\n'))

    def test_split_url_to_filename(self):
        self.assertEqual(['example.com', 'index.php_article=Main_Page'],
            util.split_url_to_filename(
                'http://example.com/index.php?article=Main_Page')
        )

        def f1():
            util.split_url_to_filename('http://example.com/../system')
        self.assertRaises(ValueError, f1)

        def f2():
            util.split_url_to_filename('http://example.com/./system')
        self.assertRaises(ValueError, f2)

    def test_append_index_filename(self):
        self.assertEqual('_index_da39a3', util.append_index_filename(''))
        self.assertEqual('index.php_index_bb6499',
            util.append_index_filename('index.php'))
        self.assertEqual(os.path.join('hello', 'index.php_index_bb6499'),
            util.append_index_filename(os.path.join('hello', 'index.php'))
        )

    def test_parse_http_date(self):
        self.assertEqual(datetime.datetime(1995, 11, 20, 19, 12, 8,
            tzinfo=datetime.timezone(datetime.timedelta(-1, 68400))),
            util.parse_http_date('Mon, 20 Nov 1995 19:12:08 -0500'))
