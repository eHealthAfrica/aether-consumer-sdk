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
import json
from operator import itemgetter
import redis
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    NamedTuple,
    Union
)

from .logger import LOG
from .settings import CONSUMER_CONFIG

DEFAULT_TENANT = CONSUMER_CONFIG.get('DEFAULT_TENANT', 'no-tenant')


class Task(NamedTuple):
    id: str
    tenant: str
    type: str
    data: Union[Dict, None] = None  # None is not set


class TaskHelper(object):

    def __init__(self, settings):
        self.settings = settings
        self.redis_db = settings.get('REDIS_DB')
        self.redis = redis.Redis(
            host=settings.get('REDIS_HOST'),
            port=settings.get('REDIS_PORT'),
            password=settings.get('REDIS_PASSWORD'),
            db=self.redis_db,
            encoding="utf-8",
            decode_responses=True
        )
        self.pubsub = None
        self._subscribe_thread = None

    # Generic Redis Task Functions
    def add(
        self,
        task: Dict[str, Any],
        type: str,
        tenant: str
    ) -> bool:
        key = '_{type}:{tenant}:{_id}'.format(
            type=type,
            _id=task['id'],
            tenant=tenant
        )
        task['modified'] = datetime.now().isoformat()
        return self.redis.set(key, json.dumps(task))

    def exists(
        self,
        _id: str,
        type: str,
        tenant: str
    ) -> bool:
        task_id = '_{type}:{tenant}:{_id}'.format(
            type=type,
            _id=_id,
            tenant=tenant
        )
        if self.redis.exists(task_id):
            return True
        return False

    def remove(
        self,
        _id: str,
        type: str,
        tenant: str
    ) -> bool:
        task_id = '_{type}:{tenant}:{_id}'.format(
            type=type,
            _id=_id,
            tenant=tenant
        )
        res = self.redis.delete(task_id)
        if not res:
            return False
        return True

    def get(
        self,
        _id: str,
        type: str,
        tenant: str
    ) -> Dict:
        task_id = f'_{type}:{tenant}:{_id}'
        task = self.redis.get(task_id)
        if not task:
            raise ValueError('No task with id {task_id}'.format(task_id=task_id))
        return json.loads(task)

    def list(
        self,
        type: str = None,
        tenant: str = DEFAULT_TENANT
    ) -> Iterable[str]:
        # ids of matching assets as a generator
        if type and not tenant:  # internal option used by jobs manager
            key_identifier = '_{type}:{tenant}:*'.format(
                type=type,
                tenant='*'
            )
            for i in self.redis.scan_iter(key_identifier):
                yield ':'.join(str(i).split(':')[-2:])  # _id
        elif type:  # normal method accessible by API
            key_identifier = '_{type}:{tenant}:*'.format(
                type=type,
                tenant=tenant
            )
            for i in self.redis.scan_iter(key_identifier):
                yield str(i).split(':')[-1]  # _id
        else:  # unused edge case
            key_identifier = f'*:{tenant}:*'
            for i in self.redis.scan_iter(key_identifier):
                yield ':'.join(
                    itemgetter(-3, -1)
                    (str(i).split(':'))
                )  # _type:_id
    # subscription tasks

    def subscribe(self, callback: Callable, pattern: str):
        if not self._subscribe_thread or not self._subscribe_thread._running:
            self._init_subscriber(callback, pattern)
        else:
            self._subscribe(callback, pattern)

    def _init_subscriber(self, callback: Callable, pattern: str):
        LOG.debug('Initializing Redis subscriber')
        self.pubsub = self.redis.pubsub()
        self._subscribe(callback, pattern)  # Must have a job first of thread dies
        self._subscribe_thread = self.pubsub.run_in_thread(sleep_time=0.1)
        LOG.debug('Subscriber Running')

    def _subscribe(self, callback: Callable, pattern: str):
        LOG.debug(f'Subscribing to {pattern}')
        keyspace = f'__keyspace@{self.redis_db}__:{pattern}'
        self.pubsub.psubscribe(**{
            f'{keyspace}': self._subscriber_wrapper(callback, keyspace)
        })
        LOG.debug(f'Added {keyspace}')

    def _subscriber_wrapper(
        self,
        fn: Callable,
        registered_channel: str
    ) -> Callable:
        # wraps the callback function so that the message instead of the event will be returned

        def wrapper(msg) -> None:
            LOG.debug(f'callback got message: {msg}')
            channel = msg['channel']
            # get _id, tenant from channel: __keyspace@0__:_test:_tenant:00001
            # where id = 00001
            keyspace, _type, tenant, _id = channel.split(':')
            redis_id = ':'.join([_type, tenant, _id])
            redis_op = msg['data']
            LOG.debug(f'Channel: {channel} received {redis_op};'
                      + f' registered on: {registered_channel}')
            if redis_op in ('set',):
                _redis_msg = self.redis.get(redis_id)
                res = Task(
                    id=_id,
                    tenant=tenant,
                    type=redis_op,
                    data=json.loads(_redis_msg)
                )
                LOG.debug(f'ID: {_id} data: {_redis_msg}')
            else:
                res = Task(
                    id=_id,
                    tenant=tenant,
                    type=redis_op
                )
            fn(res)  # On callback, hit registered function with proper data
        return wrapper

    def _unsubscribe_all(self) -> None:
        LOG.debug('Unsubscribing from all pub-sub topics')
        self.pubsub.punsubscribe()

    def stop(self, *args, **kwargs) -> None:
        self._unsubscribe_all()
        if self._subscribe_thread and self._subscribe_thread._running:
            LOG.debug('Stopping Subscriber thread.')
            self._subscribe_thread._running = False
            try:
                self._subscribe_thread.stop()
            except (
                redis.exceptions.ConnectionError,
                AttributeError
            ):
                LOG.error('Could not explicitly stop subscribe thread: no connection')
