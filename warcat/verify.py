'''Verification helpers'''
# Copyright 2013 Christopher Foo <chris.foo@gmail.com>
# Licensed under GPLv3. See COPYING.txt for details.
import hashlib
import base64
from warcat import util, model
import binascii


ALGORITHM_MAP = {
    'md5': hashlib.md5,
    'sha1': hashlib.sha1,
    'sha224': hashlib.sha224,
    'sha256': hashlib.sha256,
    'sha384': hashlib.sha384,
    'sha512': hashlib.sha512,
}


def parse_digest_field(s):
    '''Return the algorithm name and digest `bytes`'''

    algorithm, digest = s.split(':', 1)
    algorithm = algorithm.lower()
    enc_digest = digest.encode()

    try:
        digest_bytes = base64.b64decode(enc_digest)
        digest_bytes = base64.b32decode(enc_digest)
        digest_bytes = base64.b16decode(enc_digest)
    except binascii.Error as e:
        if not digest_bytes:
            raise e

    return algorithm, digest_bytes


def verify_block_digest(record):
    '''Return `True` if the content block hash digest is valid'''

    value = record.header.fields['WARC-Block-Digest']
    alg_name, given_digest = parse_digest_field(value)
    hash_obj = ALGORITHM_MAP[alg_name]()

    if isinstance(record.content_block, model.BlockWithPayload):
        content_block = record.content_block.binary_block
    else:
        content_block = record.content_block

    util.copyfile_obj(content_block.get_file(), hash_obj,
        max_length=content_block.length, write_attr_name='update')

    return given_digest == hash_obj.digest()


def verify_payload_digest(record):
    '''Return `True` if the payload hash digest is valid'''

    value = record.header.fields['WARC-Payload-Digest']
    alg_name, given_digest = parse_digest_field(value)
    hash_obj = ALGORITHM_MAP[alg_name]()
    content_block = record.content_block.payload

    util.copyfile_obj(content_block.get_file(), hash_obj,
        max_length=content_block.length, write_attr_name='update')

    return given_digest == hash_obj.digest()
