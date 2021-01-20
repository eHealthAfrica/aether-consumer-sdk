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


from abc import ABCMeta, abstractmethod
from datetime import datetime
from dataclasses import asdict, dataclass
from inspect import signature, getdoc
import json
import queue
from time import sleep
import threading
from typing import Any, Callable, Dict, List, Union

from aether.python.redis.task import Task, TaskEvent
from aether.python.utils import replace_nested
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError
from werkzeug.local import LocalProxy

from .exceptions import ConsumerHttpException
from .logger import get_logger
from .helpers import classproperty, require_property

LOG = get_logger('Resource')


# work around to make MethodDesc json serializable

def _default(self, obj):
    return getattr(obj.__class__, 'to_json', _default.default)(obj)


_default.default = json.JSONEncoder.default  # Save unmodified default.
json.JSONEncoder.default = _default  # Replace it.


@dataclass
class MethodDesc:
    method: str
    signature: str
    doc: str

    def to_json(self):
        return asdict(self)


BASE_REDIS_METHODS = {
    'READ': MethodDesc(
        method='get',
        signature='id:str',
        doc='''
        GET / POST
        Retrieve an instance of this type.
        Requires argument {id}
        '''
    ),
    'CREATE': MethodDesc(
        method='add',
        signature='json_body: ResourceDefinition',
        doc='''
        POST (json)
        Create a new instance of this type.
        Requires the Resource Definition for the new instance as json body
        '''
    ),
    'DELETE': MethodDesc(
        method='delete',
        signature='id:str',
        doc='''
        GET / POST
        Delete and instance of this type
        Requires argument {id}
        '''
    ),
    'LIST': MethodDesc(
        method='list',
        signature='()',
        doc='''
        GET
        List existing instance of this type
        '''
    ),
    'VALIDATE': MethodDesc(
        method='validate',
        signature='json_body: ResourceDefinition',
        doc='''
        POST
        Validate a ResourceDefinition against the type schema
        Requires the Resource Definition for the new instance as json body
        '''
    )
}

BASE_PUBLIC_ACTIONS = list(BASE_REDIS_METHODS.keys()) + [
    'validate_pretty',
    'describe',
    'get_schema',
    'mask_config'
]


HIDDEN_METHODS = [
    'mask_config'
]


class ResourceDefinition(dict):

    def __getattr__(self, name):
        try:
            super().__getattr__(name)
        except AttributeError:
            result = self.get(name)
            if not result:
                raise AttributeError(f'{self.__class__.__name__} has no attribute {name}')
            return result


class AbstractResource(metaclass=ABCMeta):

    @property
    @abstractmethod
    def schema(self) -> str:  # the implementation of this resource, as stringified jsonschema
        return self.schema

    @property
    @abstractmethod
    def name(self) -> str:  # must be unique per consumer!
        return self._name

    @classproperty
    def static_actions(cls) -> Dict[str, Callable]:
        return {
            'describe': cls._describe,
            'get_schema': cls._get_schema,
            'validate_pretty': cls._validate_pretty,
            'mask_config': cls._mask_config
        }

    @classproperty
    def public_actions(self) -> List[str]:  # public interfaces for this type
        return BASE_PUBLIC_ACTIONS + []

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
    def _validate_pretty(cls, definition, *args, **kwargs):
        '''
        Return a lengthy validations.
        {'valid': True} on success
        {'valid': False, 'validation_errors': [errors...]} on failure
        '''
        if isinstance(definition, LocalProxy):
            definition = definition.get_json()
        if cls._validate(definition):
            return {'valid': True}
        else:
            errors = sorted(cls.validator.iter_errors(definition), key=str)
            return {
                'valid': False,
                'validation_errors': [str(e) for e in errors]
            }

    @classmethod
    def _get_schema(cls, *args, **kwargs):
        '''
        Returns the schema for instances of this resource
        '''
        return cls.schema

    @classmethod
    def _describe(cls, *args, **kwargs) -> List[MethodDesc]:
        '''
        Described the available methods exposed by this resource type
        '''
        description: List[MethodDesc] = cls._describe_static()
        for action in cls.public_actions:
            if action in HIDDEN_METHODS:
                continue
            elif action in BASE_REDIS_METHODS.keys():
                description.append(BASE_REDIS_METHODS.get(action))
                continue
            try:
                method = getattr(cls, action)
                description.append(MethodDesc(
                    method=action,
                    signature=str(signature(method)),
                    doc=getdoc(method)
                ))
            except AttributeError:
                pass
        return description

    @classmethod
    def _describe_static(cls):
        description = []
        for name, method in cls.static_actions.items():
            if name in HIDDEN_METHODS:
                continue
            description.append(MethodDesc(
                method=name,
                signature=str(signature(method)),
                doc=getdoc(method)
            ))
        return description

    @classmethod
    def _mask_config(cls, config, *args, **kwargs):
        if not hasattr(cls, '_masked_fields') or not cls._masked_fields:
            return config
        for path in cls._masked_fields:
            parts = path.lstrip('$.').split('.')
            try:
                replace_nested(config, parts, '*****')
            except KeyError:
                pass
        return config


class ResourceReference(object):
    redis_type: str
    redis_name: str
    redis_path: str
    job_path: str
    _class: Callable  # BaseResource

    def __init__(self, name, job_path, _class):
        self.job_path = job_path
        self._class = _class
        self.make_paths(name)

    def make_paths(self, name):
        name = name.lower()
        self.redis_type = f'{name}'
        self.redis_name = f'_{name}:'
        self.redis_path = f'{self.redis_name}*'


def lock(f):
    # Unlocked / Prioritized by InstanceManager._delegate
    def wrapper(self, *args, **kwargs):
        marker = None
        priority = 5
        stamp = datetime.now()
        if '__queue_priority' in kwargs:
            priority = kwargs['__queue_priority']
            del kwargs['__queue_priority']
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
            res = f(self, *args, **kwargs)
            return res
        except Exception as err:
            raise err
        finally:
            self.lock.release()
    return wrapper


class BaseResource(AbstractResource):

    # instance vars

    id: str
    definition: ResourceDefinition  # implementation of the def described in the schema
    context: 'InstanceManager'  # reference to parent InstanceManager
    _masked_fields: List[str] = []  # jsonpaths to be masked when showing definition
    # requires no instance to execute
    validator: Any = None
    lock: threading.Lock
    waiting_line: queue.PriorityQueue
    _stopped: bool

    # class attributes

    @property
    @abstractmethod
    def jobs_path(self) -> str:  # The jsonpath to this resource in the Calling Job
        return self._jobs_path

    @classproperty
    def reference(cls) -> ResourceReference:
        _name = require_property(cls.name)
        _jobs_path = require_property(cls.jobs_path)
        return ResourceReference(_name, _jobs_path, cls)

    def __init__(self, tenant, definition, context):
        # should be validated before initialization
        self._stopped = False
        self.lock = threading.Lock()
        self.waiting_line = queue.PriorityQueue()
        self.id = definition['id']
        self.definition = ResourceDefinition(definition)
        self.tenant = tenant
        self.context = context
        self._on_init()

    def _on_init(self):
        pass

    @lock
    def update(self, definition):
        self.definition = ResourceDefinition(definition)
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
            keys = list(self.instances.keys())
            for k in keys:
                try:
                    if self._stopped:
                        return
                    res = self.instances.get(k)
                    if not res:
                        continue
                    if res.lock.locked():
                        # resource in use
                        continue
                    # if someone is in line, open them up
                    token = res.waiting_line.get(timeout=0)
                    priority, stamp, marker = token
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
            return None

    def update(self, _id, _type, tenant, body):
        key = self.format(_id, _type, tenant)
        _classes = {_cls.name: _cls for _cls in self.rules}
        _cls: BaseResource = _classes.get(_type)
        if not _cls:
            LOG.error([_type, _classes])
            raise RuntimeError(f'Expected to find definition for {_type}')
        if key in self.instances:
            # this is blocking on lock so thread it
            thread = threading.Thread(
                target=self.instances[key].update,
                args=(body, ),
                kwargs={'__queue_priority': 1},
                daemon=True)
            thread.start()
            LOG.debug(f'Updating instance of {key}')
        else:
            self.instances[key] = _cls(tenant, body, self)
            LOG.debug(f'Created new instance of {key}, now: {list(self.instances.keys())}')

    def dispatch(self, tenant=None, _type=None, operation=None, _id=None, request=None):
        LOG.debug(f'Dispatching request {tenant}:{_type}:{operation}{_id}')
        try:
            inst = self.get(_id, _type, tenant)
            if not inst:
                raise KeyError
            fn = getattr(inst, operation)
            res = fn(request)
            return res
        except KeyError:
            raise ConsumerHttpException(f'No resource of type "{_type}" with id "{_id}"', 404)
        except ConsumerHttpException as cer:
            raise cer
        except Exception as err:
            raise ConsumerHttpException(repr(err), 500)

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
        else:
            LOG.warning(f'Received out of band message: {msg}')
