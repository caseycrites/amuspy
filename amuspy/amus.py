#!/usr/bin/env python

import argparse
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

def get_parser():
    parser = argparse.ArgumentParser(
        description='Multipart upload files to Amazon S3.'
    )

    required_group = parser.add_argument_group('Required Group')
    required_group.add_argument('filepath', help='Filepath of file to upload.')
    required_group.add_argument('bucket', help='S3 bucket name.')

    creds_group = parser.add_argument_group('Credentials Group')
    creds_group.add_argument('-k', '--key', dest='key', help='AWS key.')
    creds_group.add_argument(
        '-s', '--secret', dest='secret', help='AWS secret.'
    )

    file_group = parser.add_argument_group('File Group')
    file_group.add_argument(
        '-f', '--filename', dest='filename',
        help='Use if you want a different filename in s3.'
    )

    performance_group = parser.add_argument_group('Performance Group')
    performance_group.add_argument(
        '-t', '--threads', dest='threads', type=int,
        help='Number of threads to upload the file with.'
    )
    performance_group.add_argument(
        '-m', '--mbytes', dest='mbytes', type=int,
        help='Size in megabytes the parts of the source file should be.'
    )

    return parser

def validate_input(args):
    if not os.path.exists(os.path.realpath(args.filepath)):
        argparse.ArgumentError(args.filepath, 'No such file.')

    if (args.secret if args.key else not args.secret):
        argparse.ArgumentError(
            args.key or args.secret,
            'Both AWS_KEY and AWS_SECRET must be specified.'
        )

    if args.key and args.secret:
        global AWS_KEY
        AWS_KEY = args.key
        global AWS_SECRET
        AWS_SECRET = args.secret

def init():
    parser = get_parser()
    args = parser.parse_args()
    validate_input(args)
    return args

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

def upload_file(args):
    keyname = args.filename or os.path.basename(args.filepath)

    bucket = get_bucket(args.bucket)
    upload = bucket.initiate_multipart_upload(keyname)

    threads = args.threads or max(multiprocessing.cpu_count() - 1, 1)
    mb_size = args.mbytes or  min(
            (os.path.getsize(args.filepath)/1e6) / (threads * 2.0), 50)
    with multimap(threads) as pmap:
        for _ in pmap(upload_part,
                ((upload.id, upload.key_name, upload.bucket_name, i, part)
                for (i, part) in
                enumerate(file_parts(args.filepath, mb_size), start=1))):
            pass
    upload.complete_upload()

def main():
    upload_file(init())
    return 0

if __name__ == '__main__':
    sys.exit(main())
