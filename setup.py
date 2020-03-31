#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
from setuptools import setup, find_packages

VERSION = os.environ.get('VERSION', '100.0.0')

setup(
    name='aet.consumer',
    version=VERSION,

    author='Shawn Sarwar',
    author_email='shawn.sarwar@ehealthafrica.org',

    url='https://github.com/eHealthAfrica/aether-consumer-sdk',
    description='''
        A library to consume messages from Kafka with added functionality based on
        Aether's schema metadata
    ''',
    license='Apache2 License',

    python_requires='>=3.6',
    install_requires=[
        'aether.python',
        'confluent_kafka',
        'flask',
        'jsonpath_ng',
        'jsonschema',
        'kafka-python',
        'redis',
        'requests',
        'spavro',
        'webtest',
    ],
    tests_require=[
        'fakeredis',
        'flake8',
        'pytest',
        'pytest-cov',
        'pytest-lazy-fixture',
        'pytest-runner'
    ],
    packages=find_packages(),
    namespace_packages=['aet'],
    keywords=['aet', 'aether', 'kafka', 'consumer'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
