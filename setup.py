from __future__ import absolute_import, division, print_function

import os
import versioneer
from setuptools import setup, find_packages


long_description = open('README.md').read() if os.path.exists('README.md') else ''

setup(
    name="datamine",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Aaron Walters",
    author_email="aaron.walters@cmegroup.com",
    description="CME Group Datamine Package.",
    packages=find_packages(exclude=['tests']),
    package_data={'dataminie': ['data/metadata/*.txt', 'data/metadata/*.csv']},
    long_description=long_description,
    zip_safe=False)
