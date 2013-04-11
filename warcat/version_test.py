import distutils.version
import unittest
import warcat.version


class TestVersion(unittest.TestCase):
    def test(self):
        distutils.version.StrictVersion(warcat.version.__version__)
