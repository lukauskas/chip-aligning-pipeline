# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='chipalign',

    version='0.0.1',

    description='A python chip aligning pipeline designed to reproduce Roadmap Epigenome data',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/lukauskas/chip-aligning-pipeline',

    # Author details
    author='Saulius Lukauskas',
    author_email='saulius.lukauskas13@imperial.ac.uk',

    # Choose your license
    license='LGPL-2.1',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
    ],

    # What does your project relate to?
    keywords='chip-seq alignment',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=['python-logstash==0.4.6',
                      'luigi>=2.7.5',
                      'numpy>=1.14.5',
                      'pandas>=0.23.2',
                      'sh>=1.12.14',
                      'six>=1.11.0',
                      'tblib>=1.3.2',
                      'pyyaml>=3.13',
                      'pybedtools>=0.7.10',
                      'requests>=2.19.1',
                      'tables>=3.4.4',
                      'klepto>=0.1.5.2'],

    extras_require={
        'test': ['nose', 'hypothesis'],
    },



)
