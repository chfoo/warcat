import io
import unittest
from warcat import util


class TestUtil(unittest.TestCase):
    def test_find_file_pattern(self):
        f = io.BytesIO(b'abcdefg\r\n\r\nhijklmnop')
        offset = util.find_file_pattern(f, b'\r\n\r\n', inclusive=True)
        self.assertEqual(11, offset)
        self.assertEqual(0, f.tell())
