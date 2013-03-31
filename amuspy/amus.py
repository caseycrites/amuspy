import logging
import os

import boto
from filechunkio.filechunkio import FileChunkIO

AWS_KEY = None
AWS_SECRET = None

logger = logging.getLogger(__name__)

def get_bucket(bucket_name):
    return boto.connect_s3(
        aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET
    ).lookup(bucket_name)

def file_parts(filepath, mb_size):
    offset = 0
    index = 1
    filesize = os.path.getsize(filepath)
    bytes = mb_size * 1048576
    while offset < filesize:
        yield FileChunkIO(filepath, offset=offset, bytes=bytes), index
        offset += mb_size
        index += 1

def track_upload_progress(part, i):
    def display_upload_progress(uploaded, total):
        logger.info(
            '%s, part %d: %d/%d uploaded' % (part.name, i, uploaded, total)
        )
    return display_upload_progress

def upload_part(upload, part, i):
    logger.info('Starting upload of %s, part %d' % (part.name, i))
    upload.upload_part_from_file(part, i, cb=track_upload_progress(part, i))
    logger.info('Finished upload of %s, part %d' % (part.name, i))

def _set_creds(key, secret):
    if key and secret:
        global AWS_KEY
        AWS_KEY = key
        global AWS_SECRET
        AWS_SECRET = secret

def upload_file(filepath, bucket_name, **kwargs):
    keyname = kwargs.get('filename') or os.path.basename(filepath)
    mb_size = kwargs.get('mb_size') or 50

    bucket = get_bucket(bucket_name)
    upload = bucket.initiate_multipart_upload(keyname)

    for part, index in file_parts(filepath, mb_size):
        upload_part(upload, part, index)

    upload.complete_upload()
