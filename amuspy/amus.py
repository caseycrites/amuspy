#!/usr/bin/env python

import contextlib
import functools
import glob
import multiprocessing
import os
import subprocess
import sys

import boto
from boto.s3.connection import S3Connection

AWS_KEY = None
AWS_SECRET = None

def map_wrap(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return apply(f, *args, **kwargs)
    return wrapper

def get_bucket(bucket_name):
    conn = boto.connect_s3() if not AWS_KEY else S3Connection(AWS_KEY, AWS_SECRET)
    return conn.lookup(bucket_name)

def file_parts(filename, mb_size):
    prefix = os.path.join(os.path.dirname(filename), 's3_upload')
    if subprocess.call(['split', '-b%sm' % mb_size, filename, prefix]):
        raise Exception('Could not split file %s.' % filename)
    return glob.glob('%s*' % prefix)

def create_upload(id, keyname, bucket_name):
    bucket = get_bucket(bucket_name)
    upload = boto.s3.multipart.MultiPartUpload(bucket)
    upload.key_name = keyname
    upload.id = id
    return upload

def track_upload_progress(part):
    print 'Starting upload of %s' % part
    def display_upload_progress(uploaded, total):
        print '%s: %d/%d uploaded' % (part, uploaded, total)
    return display_upload_progress

@map_wrap
def upload_part(id, keyname, bucketname, i, part):
    upload = create_upload(id, keyname, bucketname)
    with open(part) as t_handle:
        upload.upload_part_from_file(t_handle, i,
                cb=track_upload_progress(part))
    print 'Finished upload of %s.' % part
    os.remove(part)

@contextlib.contextmanager
def multimap(threads):
    pool = multiprocessing.Pool(threads)
    yield pool.imap
    pool.terminate()

def _set_creds(key, secret):
    if key and secret:
        global AWS_KEY
        AWS_KEY = key
        global AWS_SECRET
        AWS_SECRET = secret

def upload_file(filepath, bucket_name, **kwargs):
    keyname = kwargs.get('filename') or os.path.basename(filepath)
    threads = kwargs.get('threads') or max(multiprocessing.cpu_count() - 1, 1)
    mb_size = kwargs.get('mb_size') or min(
            (os.path.getsize(filepath)/1e6) / (threads * 2.0), 50)

    bucket = get_bucket(bucket_name)
    upload = bucket.initiate_multipart_upload(keyname)

    with multimap(threads) as pmap:
        for _ in pmap(upload_part,
                ((upload.id, upload.key_name, upload.bucket_name, i, part)
                for (i, part) in
                enumerate(file_parts(filepath, mb_size), start=1))):
            pass
    upload.complete_upload()
