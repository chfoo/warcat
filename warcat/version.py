short_version = '0.5'
__version__ = short_version + ''

try:
    import distutils.version
except ImportError:
    pass
else:
    distutils.version.StrictVersion(__version__)


if __name__ == '__main__':
    print(__version__, end='')
