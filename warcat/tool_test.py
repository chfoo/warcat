from warcat.tool import ListTool, VerifyTool, SplitTool, ExtractTool, ConcatTool
import glob
import os.path
import tempfile
import unittest


class TestTool(unittest.TestCase):
    test_dir = os.path.join('example')

    def test_list(self):
        tool = ListTool([os.path.join(self.test_dir, 'at.warc')])
        tool.process()

    def test_verify(self):
        tool = VerifyTool([os.path.join(self.test_dir, 'at.warc')],
            preserve_block=False)
        tool.process()

        self.assertEqual(1, tool.problems)

    def test_split(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            tool = SplitTool([os.path.join(self.test_dir, 'at.warc')],
                out_dir=temp_dir)
            tool.process()

            self.assertEqual(8, len(os.listdir(temp_dir)))

    def test_extract(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            tool = ExtractTool([os.path.join(self.test_dir, 'at.warc')],
                out_dir=temp_dir, preserve_block=False)
            tool.process()

            self.assertEqual(1, len(
                glob.glob(os.path.join(temp_dir, '*', '*index*'))))

    def test_concat(self):
        with tempfile.NamedTemporaryFile() as f:
            tool = ConcatTool([os.path.join(self.test_dir, 'at.warc')],
                out_file=f)
            tool.process()

            f.seek(0)

            tool = VerifyTool([f.name], preserve_block=False)
            tool.process()

            self.assertEqual(1, tool.problems)

