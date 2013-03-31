#!/usr/bin/env python

from setuptools import setup

setup(
    name='amuspy',
    version='0.2.0',
    author='Casey W Crites',
    author_email='crites.casey@gmail.com',
    packages=['amuspy'],
    url='http://pypi.python.org/pypi/amuspy',
    license='LICENSE',
    description='CLI for multi-part uploads to Amazon S3.',
    long_description=open('README.rst').read(),
    install_requires=[
        'boto>=2.6.0',
        'filechunkio>=1.5',
        'wsgiref>=0.1.2',
    ],
    entry_points={
        'console_scripts': [
            'amus = amuspy.cli:main',
        ]
    },
)
