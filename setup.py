from distutils.core import setup

setup(
    name='amuspy',
    version='0.1.0',
    author='Casey W Crites',
    author_email='crites.casey@gmail.com',
    packages=['amuspy'],
    url='http://pypi.python.org/pypi/amuspy',
    license='LICENSE',
    description='CLI for multi-part uploads to Amazon S3.',
    long_description=open('README.rst').read(),
    install_requires=[
        'boto>=2.6.0',
        'distribute>=0.6.30',
        'wsgiref>=0.1.2',
    ],
    entry_points={
        'console_scripts': [
            'amus = amuspy.amus:main',
        ]
    },
)
