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


from datetime import datetime
from inspect import signature
import json
import queue
from time import sleep
import threading
from typing import Any, Dict, List, Union

from aether.python.redis.task import Task, TaskEvent
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from .logger import get_logger

LOG = get_logger('Resource')


def lock(f):
    # Unlocked / Prioritized by InstanceManager._delegate
    def wrapper(self, *args, **kwargs):
        marker = None
        priority = 5
        stamp = datetime.now()
        if '__queue_priority' in kwargs:
            priority = kwargs['__queue_priority']
            del kwargs['__queue_priority']
        LOG.debug(f'{self.id} -> {f.__name__} must wait')
        marker = threading.Lock()
        marker.acquire()  # lock it and put it into the queue
        self.waiting_line.put(tuple([priority, stamp, marker]))
        # wait for the marker to be unlocked or the kill signal
        while marker.locked() and not self._stopped:
            sleep(.01)
        if self._stopped:
            marker.release()
            raise RuntimeError('Resource Stopped before action could be completed.')
        # mark the resource as used
        self.lock.acquire(blocking=False)
        marker = None

        try:
            LOG.debug(f'{self.id} servicing {priority}, {stamp}')
            res = f(self, *args, **kwargs)
            return res
        except Exception as err:
            raise err
        finally:
            LOG.debug(f'{self.id} DONE {priority}, {stamp}')
            self.lock.release()
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
    schema: str = None  # the schema of this resource type as JSONSchema
    validator: Any = None
    lock: threading.Lock
    waiting_line: queue.PriorityQueue
    _stopped: bool

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
    def get_schema(cls):
        return cls.schema

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
        self._stopped = False
        self.lock = threading.Lock()
        self.waiting_line = queue.PriorityQueue()
        self.id = definition['id']
        self.definition = definition
        self._on_init()

    def _on_init(self):
        pass

    @lock
    def update(self, definition):
        self.definition = definition
        LOG.debug(f'{self.id} got new definition')
        self._on_change()

    def _on_delete(self):
        self._stopped = True
        pass

    def _on_change(self):
        '''
        Handles changes
        '''
        pass

    def stop(self):
        '''
        Handles stop call before removal
        '''
        self._stopped = True
        self._on_stop()

    def _on_stop(self):
        pass


class InstanceManager(object):

    instances: Dict[str, BaseResource]
    rules: Dict[str, Any]
    stopped: bool

    def __init__(self, rules):
        self.rules = rules
        self.instances = {}
        self._stopped = False
        thread = threading.Thread(
            target=self.__delegate,
            daemon=True)
        thread.start()
        LOG.debug('Instance Manager started')

    def __delegate(self):
        while not self._stopped:
            for k, res in self.instances.items():
                try:
                    if self._stopped:
                        return
                    if res.lock.locked():
                        # resource in use
                        continue
                    # if someone is in line, open them up
                    token = res.waiting_line.get(timeout=0)
                    priority, stamp, marker = token
                    LOG.debug(f'Tapping {priority}, {stamp}')
                    marker.release()
                    res.lock.acquire(blocking=False)
                except queue.Empty:
                    pass
                except AttributeError:
                    # no lock or line
                    pass
            sleep(.01)

    def stop(self):
        self._stopped = True
        LOG.info('Stopping Instances')
        keys = list(self.instances.keys())
        for k in keys:
            LOG.debug(f'Stopping {k}')
            thread = threading.Thread(
                target=self.__remove_on_unlock,
                args=(k, ),
                daemon=True)
            thread.start()
            yield thread

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
            LOG.warning(f'Expected key missing {key} : {list(self.instances.keys())}')
            return None

    def update(self, _id, _type, tenant, body):
        key = self.format(_id, _type, tenant)
        _cls = self.rules.get(_type, {}).get('class')
        if key in self.instances:
            # this is blocking on lock so thread it
            thread = threading.Thread(
                target=self.instances[key].update,
                args=(body, ),
                kwargs={'__queue_priority': 1},
                daemon=True)
            thread.start()
        else:
            self.instances[key] = _cls(body)
            LOG.debug(f'Created new instance of {key}, now: {list(self.instances.keys())}')

    def dispatch(self, tenant=None, _type=None, operation=None, request=None):
        pass

    def format(self, _id, _type, tenant):
        return f'{tenant}:{_type}:{_id}'

    def remove(self, _id, _type, tenant):
        key = self.format(_id, _type, tenant)
        LOG.debug(f'removing resource {key}')
        if key in self.instances:
            self.instances[key]._on_delete()
            thread = threading.Thread(
                target=self.__remove_on_unlock,
                args=(key, ),
                daemon=True)
            thread.start()
        else:
            LOG.error(f'Cannot remove resource with key {key} -- {list(self.instances.keys())}')
        return True

    def __remove_on_unlock(self, key):
        try:
            obj = self.instances[key]
            obj.stop()
            # safe to delete
            del self.instances[key]
            LOG.debug(f'{key} removed')
        except KeyError:
            LOG.error(f'KE on {key}')

    def on_resource_change(self, msg: Union[Task, TaskEvent]) -> None:
        if isinstance(msg, Task):
            LOG.debug(f'Received Task: "{msg.type}" on job: {msg.id}')
            _type = '_'.join(msg.type.split('_')[1:])
            _id = msg.id
            body = msg.data
            tenant = msg.tenant
            self.update(_id, _type, tenant, body)
        elif isinstance(msg, TaskEvent):
            LOG.debug(f'Received TaskEvent: "{msg}"')
            if msg.event == 'del':
                _type = '_'.join(msg.type.split('_')[1:])
                self.remove(msg.task_id, _type, msg.tenant)
