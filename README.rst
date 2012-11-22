====
amus
====

amus is a command-line utility for doing multipart uploads to Amazon S3.

amus is mostly a bastardization of the script found here_ with some cli slapped on top of it.

.. _here: https://github.com/chapmanb/cloudbiolinux/blob/master/utils/s3_multipart_upload.py

amus, like a fish as a prize at a carnival, is very beta.

Basic Usage
===========

amus has a few extra options, but if you have your environment set up for AWS, it's as easy as::

    $ amus path/to/file name_of_s3_bucket

Supplying AWS creds
===================

If you don't have your AWS_ACCESS_KEY_ID and AWS_ACCESS_KEY_SECRET environment variables set, you can pass them in like so::

    $ amus -k AWS_ACCESS_KEY_ID -s AWS_ACCESS_KEY_SECRET path/to/file name_of_s3_bucket

Changing Output
===============

By default, amus will name the file the same thing on S3 as it is locally. If you want to name it differently, simply supply -f option, like so::

    $ amus -f way_cooler_name path/to/file name_of_s3_bucket

Tweaking Performance
====================

If you want to change the # of threads amus uses (# of cpu cores, by default), supply the -t option::

    $ amus -t 2 path/to/file name_of_s3_bucket

or if the size of the parts (50mb, by default) isn't what you want, supply the -m option with a size in megabytes::

    $ amus -m 100 path/to/file name_of_s3_bucket
