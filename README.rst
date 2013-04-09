WARCAT: Web ARChive (WARC) Archiving Tool
=========================================

Tool and library for handling Web ARChive (WARC) files.


Quick Start
===========

Requirements:

* Python 3


Install dependencies::

    pip-3 install -r requirements.txt


Install (optional)::

    python3 setup.py install


Run::
    
    python3 -m warcat --help
    python3 -m warcat list example/at.warc.gz


Supported commands
++++++++++++++++++

concat
    Naively join archives into one
help
    List commands available
list
    List contents of archive
pass
    Load archive and write it back out
split
    Split archives into individual records


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
    >>> record.content_block.payload.size
    0
    >>> bytes(warc)[:60]
    b'WARC/1.0\r\nWARC-Type: warcinfo\r\nContent-Type: application/war'
    >>> bytes(record.content_block.fields)[:60]
    b'software: Wget/1.13.4-2608 (linux-gnu)\r\nformat: WARC File Fo'


About
=====


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

To-do
+++++

* Verify hash digests
* Conformance checking
* Smart archive join
* etc.

