import io
import unittest
from warcat import util


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
