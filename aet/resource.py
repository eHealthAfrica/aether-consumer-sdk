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


from inspect import signature
import json
import threading
from typing import Any, Dict, List, TYPE_CHECKING
from uuid import uuid4

from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from aether.python.redis.task import TaskHelper

from .logger import get_logger

LOG = get_logger('Resource')


def lock(f):
    def wrapper(*args, **kwargs):
        args[0].lock.acquire()
        res = f(*args, **kwargs)
        args[0].lock.release()
        return res
    return wrapper


class BaseResource(object):
    __id: str
    definition: Any  # the implementation of this resource
    # requires no instance to execute
    static_actions: Dict[str, str] = {
        'describe': '_describe',
        'validate': '_validate_pretty'
    }
    public_actions: List[str]  # public interfaces for this type
    schema: str  # the schema of this resource type as JSONSchema
    validator: Any = None
    lock: threading.Lock

    @classmethod
    def _validate(cls, definition) -> bool:
        if not cls.validator:
            cls.validator = Draft7Validator(json.loads(cls.schema))
        try:
            cls.validator.validate(definition)
            return True
        except ValidationError:
            return False

    @classmethod
    def _validate_pretty(cls, definition):
        if cls._validate(definition):
            return {'valid': True}
        else:
            errors = sorted(cls.validator.iter_errors(definition), key=str)
            return {
                'valid': False,
                'validation_errors': [e for e in errors]
            }

    @classmethod
    def _describe(cls):
        description = []
        for action in cls.public_actions:
            try:
                method = getattr(cls, action)
                item = {'method': action}
                item['signature'] = str(signature(method))
                description.append(item)
            except AttributeError:
                LOG.error(f'{cls.__name__} has no method {action}')
                pass
        return description

    @classmethod
    def _describe_static(cls):
        description = []
        for name, action in cls.static_actions.items():
            try:
                method = getattr(cls, action)
                item = {'method': name}
                item['signature'] = str(signature(method))
                description.append(item)
            except AttributeError:
                LOG.error(f'{cls.__name__} has no method {action}')
                pass
        return description

    def __init__(self, definition):
        # should be validated before initialization
        self.lock = threading.Lock()
        self.__id = str(uuid4())
        self.definition = definition

    @lock
    def update(self, definition):
        pass

    def _on_change(self):
        '''
        Locks methods until update
        '''
        pass


class InstanceManager(object):

    instances: Dict[str, BaseResource]
    rules: Dict[str, Any]
    task: TaskHelper

    def init(self, rules, helper):
        self.rules = rules
        self.task = helper

    def get(self, _id, _type):
        '''
        Get a resource class instance by name and ID
        '''
        key = self.format(_id, _type)
        return self.instances[key]

    def update(self, _id, _type, body):
        key = self.format(_id, _type)
        _cls = self.rules.get(_type, {}).get('class')
        if key in self.instances:
            # this is blocking on lock so thread it
            thread = threading.Thread(
                target=self.instances[key].update,
                args=(body, ))
            thread.start()
        else:
            self.instances[key] = _cls(body)

    def dispatch(self, tenant=None, _type=None, operation=None, request=None):
        pass

    def format(_id, _type):
        return f'{_type}:{_id}'
