from distutils.core import setup

import warcat.version

setup(name='Warcat',
    version=warcat.version.__version__,
    description='Tool and library for handling Web ARChive (WARC) files.',
    author='Christopher Foo',
    author_email='chris.foo@gmail.com',
    url='https://github.com/chfoo/warcat',
    packages=[
        'warcat',
        'warcat.model',
    ],
    install_requires=[
        'isodate',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Topic :: System :: Archiving',
    ],
)
