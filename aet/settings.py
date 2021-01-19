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

import json
import os


class Settings(dict):
    # A container for our settings
    def __init__(self, file_path=None, alias=None, exclude=None):
        # takes precedent over env and initial values. Case sensitive
        # useful for tests
        self.overrides = {}
        if not exclude:
            self.exclude = []
        else:
            self.exclude = [k.upper() for k in exclude]
        self.alias = alias if alias else {}
        if file_path:
            self.load(file_path)

    def get(self, key, default=None):
        try:
            return self.__getitem__(key.upper())
        except KeyError:
            return default

    def override(self, key, value):
        self.overrides[key] = value

    def __getattr__(self, name):
        try:
            if name in self.overrides:
                return self.overrides[name]
            super().__getattr__(name)
        except AttributeError:
            return self.get(name)

    def __getitem__(self, key):
        if key in self.overrides:
            return self.overrides[key]
        if self.alias and key in self.alias:
            key = self.alias.get(key)
        result = os.environ.get(key.upper())
        if result is None:
            result = super().__getitem__(key.upper())

        return result

    def copy(self):
        keys = [k for k in self.keys() if k not in self.exclude]
        for key in self.alias:
            keys.append(key)
        return {k: self.get(k) for k in keys}

    def load(self, path):
        with open(path) as f:
            obj = json.load(f)
            for k in obj:
                self[k.upper()] = obj.get(k)


def check_required_fields(conf, fields):
    fields = json.loads(fields)
    missing = []
    found = []
    for f in fields:
        if not conf.get(f):
            missing.append(f)
        else:
            found.append(f)
    assert missing == [], 'Required fields are missing: %s' % (missing)
    return found


CONSUMER_CONFIG: Settings = Settings(file_path=os.environ.get('CONSUMER_CONFIG_PATH', None))
KAFKA_CONFIG: Settings = Settings(file_path=os.environ.get('KAFKA_CONFIG_PATH', None))

check_required_fields(CONSUMER_CONFIG, os.environ.get('REQUIRED_CONSUMER_CONFIG', '[]'))
check_required_fields(KAFKA_CONFIG, os.environ.get('REQUIRED_KAFKA_CONFIG', '[]'))
