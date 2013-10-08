WARCAT: Web ARChive (WARC) Archiving Tool
=========================================

Tool and library for handling Web ARChive (WARC) files.


Quick Start
===========

Requirements:

* Python 3

Install stable version::

    pip-3 install warcat

Or install latest version::

    git clone git://github.com/chfoo/warcat.git
    pip-3 install -r requirements.txt
    python3 setup.py install


Example Run::

    python3 -m warcat --help
    python3 -m warcat list example/at.warc.gz
    python3 -m warcat verify megawarc.warc.gz --progress
    python3 -m warcat extract megawarc.warc.gz --output-dir /tmp/megawarc/ --progress


Supported commands
++++++++++++++++++

concat
    Naively join archives into one
extract
    Extract files from archive
help
    List commands available
list
    List contents of archive
pass
    Load archive and write it back out
split
    Split archives into individual records
verify
    Verify digest and validate conformance


Library
+++++++

Example:

.. code-block:: python

    >>> import warcat.model
    >>> warc = warcat.model.WARC()
    >>> warc.load('example/at.warc.gz')
    >>> len(warc.records)
    8
    >>> record = warc.records[0]
    >>> record.warc_type
    'warcinfo'
    >>> record.content_length
    233
    >>> record.header.version
    '1.0'
    >>> record.header.fields.list()
    [('WARC-Type', 'warcinfo'), ('Content-Type', 'application/warc-fields'), ('WARC-Date', '2013-04-09T00:11:14Z'), ('WARC-Record-ID', '<urn:uuid:972777d2-4177-4c63-9fde-3877dacc174e>'), ('WARC-Filename', 'at.warc.gz'), ('WARC-Block-Digest', 'sha1:3C6SPSGP5QN2HNHKPTLYDHDPFYKYAOIX'), ('Content-Length', '233')]
    >>> record.header.fields['content-type']
    'application/warc-fields'
    >>> record.content_block.fields.list()
    [('software', 'Wget/1.13.4-2608 (linux-gnu)'), ('format', 'WARC File Format 1.0'), ('conformsTo', 'http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf'), ('robots', 'classic'), ('wget-arguments', '"http://www.archiveteam.org/" "--warc-file=at" ')]
    >>> record.content_block.fields['software']
    'Wget/1.13.4-2608 (linux-gnu)'
    >>> record.content_block.payload.length
    0
    >>> bytes(warc)[:60]
    b'WARC/1.0\r\nWARC-Type: warcinfo\r\nContent-Type: application/war'
    >>> bytes(record.content_block.fields)[:60]
    b'software: Wget/1.13.4-2608 (linux-gnu)\r\nformat: WARC File Fo'


.. note::

    The library may not be entirely thread-safe yet.


About
=====

The goal of the Warcat project is to create a tool and library as easy and fast as manipulating any other archive such as tar and zip archives.

Warcat is designed to handle large, gzip-ed files by partially extracting them as needed.

Warcat is provided without warranty and cannot guarantee the safety of your files. Remember to make backups and test them!


* Homepage: https://github.com/chfoo/warcat
* Documentation: http://warcat.readthedocs.org/
* Questions?: https://answers.launchpad.net/warcat
* Bugs?: https://github.com/chfoo/warcat/issues
* PyPI: https://pypi.python.org/pypi/Warcat/
* Chat: irc://irc.efnet.org/archiveteam-bs (I'll be on #archiveteam-bs on EFnet) 


Specification
+++++++++++++

This implementation is based loosely on draft ISO 28500 papers ``WARC_ISO_28500_version1_latestdraft.pdf`` and ``warc_ISO_DIS_28500.pdf`` which can be found at http://bibnum.bnf.fr/WARC/ .


File format
+++++++++++

Here's a quick description:

A WARC file contains one or more Records concatenated together. Each Record contains Named Fields, newline, a Content Block, newline, and newline. A Content Block may be two types: {binary data} or {Named Fields, newline, and binary data}. Named Fields consists of string, colon, string, and newline.

A Record may be compressed with gzip. Filenames ending with ``.warc.gz`` indicate one or more gzip compressed files concatenated together.


Alternatives
++++++++++++

Warcat is inspired by

* https://github.com/internetarchive/warc
* http://code.hanzoarchives.com/warc-tools


Development
===========

.. image:: https://travis-ci.org/chfoo/warcat.png
    :target: https://travis-ci.org/chfoo/warcat
    :alt: Travis build status


Testing
+++++++

Always remember to test. Continue testing::

    python3 -m unittest discover -p '*_test.py'
    nosetests3


To-do
+++++

* Smart archive join
* Regex filtering of records
* Generate index to disk (eg, for fast resume)
* Grab files like wget and archive them
* See TODO and FIXME markers in code
* etc.

