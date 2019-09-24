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
from typing import Any, Dict, List, Union

from aether.python.redis.task import Task, TaskEvent
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from .logger import get_logger

LOG = get_logger('Resource')


def lock(f):
    def wrapper(self, *args, **kwargs):
        if not self.lock.acquire(timeout=type(self).lock_timeout_sec):
            raise RuntimeError(f'Could not acquire lock for method {f.__name__} in time.')
        try:
            res = f(self, *args, **kwargs)
            return res
        except Exception as err:
            raise err
        finally:
            try:
                self.lock.release()
            except RuntimeError:
                LOG.debug('Tried to release open lock.')
    return wrapper


class BaseResource(object):
    id: str
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
    lock_timeout_sec: int = 60

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
                'validation_errors': [str(e) for e in errors]
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
        self.id = definition['id']
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

    def init(self, rules):
        self.rules = rules

    def _on_init(self):
        pass

    def exists(self, _id, _type, tenant):
        key = self.format(_id, _type, tenant)
        return key in self.instances

    def get(self, _id, _type, tenant):
        '''
        Get a resource class instance by name and ID
        '''
        key = self.format(_id, _type, tenant)
        try:
            return self.instances[key]
        except KeyError:
            return None

    def update(self, _id, _type, tenant, body):
        key = self.format(_id, _type, tenant)
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

    def format(_id, _type, tenant):
        return f'{tenant}:{_type}:{_id}'

    def remove(self, _id, _type, tenant):
        key = self.format(_id, _type, tenant)
        if key in self.instances:
            thread = threading.Thread(
                target=self.__remove_on_unlock,
                args=(key, ))
            thread.start()
        return True

    def __remove_on_unlock(self, key):
        try:
            obj = self.instances[key]
            lock = obj.lock
            lock.acquire()
            # safe to delete
            del self.instances[key]
            lock.release()
        except KeyError:
            pass

    def on_resource_change(self, msg: Union[Task, TaskEvent]) -> None:
        if isinstance(msg, Task):
            LOG.debug(f'Received Task: "{msg.type}" on job: {msg.id}')
            job = msg.data
            tenant = msg.tenant
            if job:  # type checker gets mad without the check here
                self._init_job(job, tenant)
        elif isinstance(msg, TaskEvent):
            LOG.debug(f'Received TaskEvent: "{msg}"')
            if msg.event == 'del':
                self.remove(msg.task_id, msg.type, msg.tenant)
