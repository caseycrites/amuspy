#!/usr/bin/env python

import contextlib
import functools
import glob
import multiprocessing
from optparse import OptionParser, OptionGroup
import os
import subprocess
import sys

import boto
from boto.s3.connection import S3Connection

AWS_KEY = None
AWS_SECRET = None

def get_parser():
    parser = OptionParser(usage=('usage: %prog [-k AWS KEY -s AWS_SECRET]
        [-f FILENAME] [-t THREADS] [-m MEGABYTES] file bucket'))

    creds_group = OptionGroup(parser, 'Credentials Group')
    creds_group.add_option('-k', '--key', dest='key', help='AWS key.')
    creds_group.add_option('-s', '--secret', dest='secret', help='AWS secret.')

    file_group = OptionGroup(parser, 'File Group')
    file_group.add_option('-f', '--filename', dest='filename',
            help='Filename for uploaded file.')

    performance_group = OptionGroup(parser, 'Performance Group')
    performance_group.add_option('-t', '--threads', dest='threads',
            help='Number of threads to upload the file with.')
    performance_group.add_option('-m', '--mbytes', dest='mbytes',
            help='Size in megabytes the parts of the source file should be.')

    parser.add_option_group(creds_group)
    parser.add_option_group(file_group)
    parser.add_option_group(performance_group)

    return parser

def validate_input(opts, args):
    if len(args) != 2:
        parser.error('You must specify a file and a bucket.')

    if not os.path.exists(os.path.realpath(args[0])):
        parser.error('Cannot find %s' % args[0])

    if (opts.secret if opts.key else not opts.secret):
        parser.error('Both AWS_KEY and AWS_SECRET must be specified.')

    if opts.key and opts.secret:
        global AWS_KEY
        AWS_KEY = opts.key
        global AWS_SECRET
        AWS_SECRET = opts.secret

def init():
    parser = get_parser()
    (opts, args) = parser.parse_args()
    validate_input(opts, args)
    return (opts, args)

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

def upload_file(opts, args):
    filepath = os.path.realpath(args[0])
    keyname = opts.filename or os.path.basename(filepath)
    bucket_name = args[1]

    bucket = get_bucket(bucket_name)
    upload = bucket.initiate_multipart_upload(keyname)

    threads = opts.threads or max(multiprocessing.cpu_count() - 1, 1)
    mb_size = opts.mbytes or  min(
            (os.path.getsize(filepath)/1e6) / (threads * 2.0), 50)
    with multimap(threads) as pmap:
        for _ in pmap(upload_part,
                ((upload.id, upload.key_name, upload.bucket_name, i, part)
                for (i, part) in
                enumerate(file_parts(filepath, mb_size), start=1))):
            pass
    upload.complete_upload()

def main():
    (opts, args) = init()
    upload_file(opts, args)
    return 0

if __name__ == '__main__':
    sys.exit(main())
