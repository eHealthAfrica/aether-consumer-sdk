#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
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


import logging
from .settings import CONSUMER_CONFIG


def get_logger(name):
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        f'%(asctime)s [AET][{name}] %(levelname)-8s %(message)s'))
    logger.addHandler(handler)
    level = logging.getLevelName(CONSUMER_CONFIG.get('log_level', 'DEBUG'))
    logger.setLevel(level)
    return logger


def wrap_logger(logger, name):
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        f'%(asctime)s [AET][{name}] %(levelname)-8s %(message)s'))
    logger.addHandler(handler)
    level = logging.getLevelName(CONSUMER_CONFIG.get('log_level', 'DEBUG'))
    logger.setLevel(level)
