import logging
import os

import boto
from filechunkio.filechunkio import FileChunkIO

logger = logging.getLogger(__name__)

def _get_bucket(bucket_name, key=None, secret=None):
    return boto.connect_s3(
        aws_access_key_id=key, aws_secret_access_key=secret
    ).lookup(bucket_name)

def _file_parts(filepath, mb_size):
    offset = 0
    index = 1
    filesize = os.path.getsize(filepath)
    bytes = mb_size * 1048576
    logger.info('%s will produce %d %d mb parts.' %
        (filepath, (filesize/bytes)+1, mb_size)
    )
    while offset < filesize:
        yield FileChunkIO(filepath, offset=offset, bytes=bytes), index
        offset += bytes
        index += 1

def _track_upload_progress(part, i):
    def log_progress(uploaded, total):
        logger.info(
            '%s, part %d: %d/%d uploaded' % (part.name, i, uploaded, total)
        )
    return log_progress

def _upload_part(upload, part, i):
    logger.info('Starting upload of %s, part %d' % (part.name, i))
    upload.upload_part_from_file(part, i, cb=_track_upload_progress(part, i))
    logger.info('Finished upload of %s, part %d' % (part.name, i))

def upload_file(filepath, bucket_name, **kwargs):
    keyname = kwargs.get('filename') or os.path.basename(filepath)
    mb_size = kwargs.get('mb_size') or 50

    bucket = _get_bucket(
        bucket_name, key=kwargs.get('key'), secret=kwargs.get('secret')
    )
    upload = bucket.initiate_multipart_upload(keyname)

    for part, index in _file_parts(filepath, mb_size):
        _upload_part(upload, part, index)

    upload.complete_upload()
