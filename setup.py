from setuptools import setup, find_packages

with open('README.md') as fp:
    long_description = fp.read()

setup(
    name="datamine",
    version="0.21.post1",
    description="CME Group DataMine Python Package",
    url="https://github.com/CMEGroup/datamine_python",
    author="Aaron Walters",
    author_email="aaron.walters@cmegroup.com",
    maintainer="Hamza Amjad",
    maintainer_email="hamza.amjad@cmegroup.com",
    license="BSD 3-Clause",
    install_requires=['requests', 'urllib3', 'pandas', 'tqdm', 'futures'],
    packages=find_packages(exclude=['tests']),
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ])
