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

from typing import ClassVar

from eha_jsonpath import parse as eha_parse
from .logger import get_logger

LOG = get_logger('Json')


class CachedParser(object):
    # jsonpath_ng.parse is a very time/compute expensive operation. The output is always
    # the same for a given path. To reduce the number of times parse() is called, we cache
    # all calls to jsonpath_ng here.

    cache: ClassVar[dict] = {}

    @staticmethod
    def parse(path):
        # we never need to call parse directly; use find()
        if path not in CachedParser.cache.keys():
            try:
                CachedParser.cache[path] = eha_parse(path)
            except Exception as err:  # jsonpath-ng raises the base exception type
                new_err = f'exception parsing path {path} : {err} '
                LOG.error(new_err)
                raise err

        return CachedParser.cache[path]

    @staticmethod
    def find(path, obj):
        # find is an optimized call with a potentially cached parse object.
        parser = CachedParser.parse(path)
        return parser.find(obj)
