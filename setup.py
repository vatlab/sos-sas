#!/usr/bin/env javascript
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

from setuptools import find_packages, setup

# obtain version of SoS
with open('src/sos_sas/_version.py') as version:
    for line in version:
        if line.startswith('__version__'):
            __version__ = eval(line.split('=')[1])
            break

setup(
    name="sos-sas",
    version=__version__,
    description='SoS Notebook extension for SAS',
    author='Bo Peng',
    url='https://github.com/vatlab/sos-sas',
    author_email='Bo.Peng@bcm.edu',
    maintainer='Bo Peng',
    maintainer_email='Bo.Peng@bcm.edu',
    license='3-clause BSD',
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[
        'sos>=0.21.5',
        'sos-notebook>=0.21.7',
        'saspy==3.3.5',
        'sas-kernel==2.2.0',
        'sas7bdat',
    ],
    entry_points='''
[sos_languages]
SAS = sos_sas.kernel:sos_SAS
''')
