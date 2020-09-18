# -*- coding: utf-8 -*-
import re
import os.path as op
from setuptools import setup


def read(filename):
    ''' Return the file content. '''
    with open(op.join(op.abspath(op.dirname(__file__)), filename)) as fd:
        return fd.read()


def get_version():
    return re.compile(r".*__version__ = '(.*?)'", re.S)\
             .match(read(op.join('fswalk', '__init__.py'))).group(1)


setup(
    name='fswalk',
    author='Bruno Bzeznik',
    author_email='Bruno.Bzeznik@univ-grenoble-alpes.fr',
    version=get_version(),
    url='https://github.com/bzizou/fs_walk',
    install_requires=[
        'requests',
        'pyjson5',
        'elasticsearch',
    ],
    packages=['fswalk'],
    zip_safe=False,
    description='An efficient multiprocessing directory walk and search tool',
    license="GNU GPL v3",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)',  # noqa
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
    ],
    entry_points='''
        [console_scripts]
        fswalk=fswalk.main:main
    ''',
)
