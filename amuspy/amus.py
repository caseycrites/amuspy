import glob
import os
import subprocess
import sys

import boto
from boto.s3.connection import S3Connection

AWS_KEY = None
AWS_SECRET = None

def get_bucket(bucket_name):
    conn = boto.connect_s3() if not AWS_KEY else S3Connection(AWS_KEY, AWS_SECRET)
    return conn.lookup(bucket_name)

def file_parts(filename, mb_size):
    prefix = os.path.join(os.path.dirname(filename), 's3_upload:')
    if subprocess.call(['split', '-b%sm' % mb_size, filename, prefix]):
        raise Exception('Could not split file %s.' % filename)
    return sorted(glob.glob('%s*' % prefix))

def track_upload_progress(part):
    print 'Starting upload of %s' % part
    def display_upload_progress(uploaded, total):
        print '%s: %d/%d uploaded' % (part, uploaded, total)
    return display_upload_progress

def upload_part(upload, i, part):
    with open(part) as t_handle:
        upload.upload_part_from_file(t_handle, i,
                cb=track_upload_progress(part))
    print 'Finished upload of %s.' % part
    os.remove(part)

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

    i = 1
    for part in file_parts(filepath, mb_size):
        upload_part(upload, i, part)
        i += 1

    upload.complete_upload()
