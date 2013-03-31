====
amus
====

amus is a tool for doing multipart uploads to Amazon S3.

amus, like a fish as a prize at a carnival, is very beta.

CLI Usage
===========

basic
  amus has a few extra options, but if you have your environment set up for AWS, it's as easy as::

    $ amus path/to/file name_of_s3_bucket

supplying AWS creds
  If you don't have your AWS_ACCESS_KEY_ID and AWS_ACCESS_KEY_SECRET environment variables set, you can pass them in like so::

    $ amus -k AWS_ACCESS_KEY_ID -s AWS_ACCESS_KEY_SECRET path/to/file name_of_s3_bucket

changing output
  By default, amus will name the file the same thing on S3 as it is locally. If you want to name it differently, simply supply -f option, like so::

    $ amus -f way_cooler_name path/to/file name_of_s3_bucket

tweaking performance
  If you want to change the size of the file parts from the 50mb default, supply the -m option with a size in megabytes::

    $ amus -m 100 path/to/file name_of_s3_bucket
