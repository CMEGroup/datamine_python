from __future__ import absolute_import, division, print_function

import versioneer
from setuptools import setup, find_packages

with open('README.md') as fp:
    long_description = fp.read()

setup(
    name="datamine",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Aaron Walters",
    author_email="aaron.walters@cmegroup.com",
    description="CME Group Datamine Package.",
    packages=find_packages(exclude=['tests']),
    package_data={'datamine': ['data/metadata/*.txt', 'data/metadata/*.csv']},
    long_description=long_description,
    zip_safe=False)
