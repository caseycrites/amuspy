#!/usr/bin/evn python

import argparse
import os

from amus import upload_file

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

def init():
    args = get_parser().parse_args()
    validate_input(args)
    return args

def main():
    args = init()
    upload_file(
        args.filepath, args.bucket,
        filename=args.filename, threads=args.threads, mb_size=args.mbytes,
        key=args.key, secret=args.secret
    )
    return 0

if __name__ == '__main__':
    sys.exit(main())
