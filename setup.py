#!/usr/bin/env python
import codecs
import os
import re
from setuptools import find_packages
from distutils.core import setup


def read(*parts):
    filename = os.path.join(os.path.dirname(__file__), *parts)
    return codecs.open(filename, encoding='utf8').read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

requirements = ['pika>=0.9']

setup(
    name='python-captain-shove',
    description='Server-side daemon that listens to commands from Captain.',
    long_description=read('README.rst'),
    version=find_version('shove/__init__.py'),
    packages=find_packages(),
    author='',
    author_email='dev-webdev@mozilla.com',
    url='https://github.com/mozilla/shove',
    license='MPL v2.0',
    install_requires=requirements,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    entry_points={
        'console_scripts': [
            'shove=shove.cmd:main'
        ]
    }
)
