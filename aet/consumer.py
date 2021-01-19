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

import json
from time import sleep
from typing import Any, ClassVar, Dict
import sys
import threading
import traceback

from aether.python.redis.task import TaskHelper
from flask import Response
import redis

from .api import APIServer
from .logger import get_logger
from .job import JobManager
from .settings import Settings

LOG = get_logger('Consumer')

EXCLUDED_TOPICS = ['__confluent.support.metrics']


class BaseConsumer(object):

    # classes used by this consumer
    _classes: ClassVar[Dict[str, Any]]

    api: APIServer
    consumer_settings: Settings
    kafka_settings: Settings
    job_manager: JobManager
    task: TaskHelper

    @classmethod
    def get_redis(cls, settings):
        return redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            password=settings.REDIS_PASSWORD,
            db=settings.REDIS_DB,
            encoding='utf-8',
            decode_responses=False
        )

    def __init__(self, CON_CONF, KAFKA_CONF, job_class, redis_instance=None):

        self.consumer_settings = CON_CONF
        self.kafka_settings = KAFKA_CONF
        if not redis_instance:
            redis_instance = type(self).get_redis(CON_CONF)
        self._classes = {
            _cls.name: _cls for _cls in job_class._resources
        }
        self._classes['job'] = job_class
        self.task = TaskHelper(
            self.consumer_settings,
            redis_instance=redis_instance)
        self.job_manager = JobManager(self.task, job_class=job_class)
        self.serve_api(self.consumer_settings)

    # Control API

    def serve_api(self, settings):
        self.api = APIServer(self, self.task, settings)
        self.api.serve()

    def stop(self, *args, **kwargs):
        LOG.info('Shutting down')
        for service_name in ['api', 'task', 'job_manager']:
            try:
                service = getattr(self, service_name)
                service.stop()
            except AttributeError:
                LOG.error(f'Consumer could not stop service {service_name}')
        sleep(.25)
        LOG.info('Shutdown Complete')

    def healthcheck(self, *args, **kwargs):
        idle_times = self.job_manager.status()
        max_idle = self.consumer_settings.get('MAX_JOB_IDLE_SEC', 600)
        if not idle_times:
            return {}
        LOG.info(f'idle times (s) {idle_times}')
        expired = {k: v for k, v in idle_times.items() if v > max_idle}
        if expired:
            LOG.error(f'Expired threads (s) {expired}')
            if self.consumer_settings.get('DUMP_STACK_ON_EXPIRE', False):
                LOG.critical('Healthcheck failed! Dumping stack.')
                self.dump_stack()
        return expired

    def dump_stack(self):
        _names = {th.ident: th.name for th in threading.enumerate()}
        _threads = []
        for _id, stack in sys._current_frames().items():
            _info = {
                'id': _id,
                'name': _names.get(_id)}
            frames = []
            for filename, lineno, name, line in traceback.extract_stack(stack):
                frames.append({
                    'filename': filename,
                    'line_no': lineno,
                    'name': name,
                    'line': line.strip() or None})
            _info['frames'] = frames
            _threads.append(_info)
        LOG.error(json.dumps(_threads, indent=2))

    # Generic API Functions that aren't pure delegation to Redis

    def validate(self, job, _type=None, verbose=False, tenant=None):
        # consumer the tenant argument only because other methods need it
        _cls = self._classes.get(_type)
        if not _cls:
            return {'error': f'un-handled type: {_type}'}
        if verbose:
            return _cls._validate_pretty(job)
        else:
            return _cls._validate(job)

    def dispatch(self, tenant=None, _type=None, operation=None, request=None):
        _fn = self._classes.get(_type).static_actions.get(operation)
        if _fn:
            return _fn(request)
        # not a static function, needs an instance
        _id = request.values.get('id', None)
        if not _id:
            LOG.debug('Request is missing ID')
            return Response('Argument "id" is required', 400)
        if _type != 'job':
            return self.job_manager.dispatch_resource_call(
                tenant,
                _type,
                operation,
                _id,
                request
            )
        return self.job_manager.dispatch_job_call(
            tenant,
            _type,
            operation,
            _id,
            request
        )
