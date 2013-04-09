from distutils.core import setup

import warcat.version

setup(name='Warcat',
    version=warcat.version.__version__,
    description='Tool and library for handling Web ARChive (WARC) files.',
    author='Christopher Foo',
    author_email='chris.foo@gmail.com',
#    url='',
    packages=['warcat'],
    classifiers=['Programming Language :: Python :: 3',]
)
