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

from time import sleep
from typing import Any, ClassVar, Dict, List, Union

from aether.python.redis.task import TaskHelper
import redis

from .api import APIServer
from .logger import get_logger
from .job import JobManager, BaseJob
from .resource import BaseResource
from .settings import Settings

LOG = get_logger('Consumer')

EXCLUDED_TOPICS = ['__confluent.support.metrics']


class BaseConsumer(object):

    # classes used by this consumer
    _classes: ClassVar[Dict[str, Any]] = {
        'resource': BaseResource,
        'job': BaseJob
    }

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
        pass

    def __init__(self, CON_CONF, KAFKA_CONF, redis_instance=None):

        self.consumer_settings = CON_CONF
        self.kafka_settings = KAFKA_CONF
        if not redis_instance:
            redis_instance = type(self).get_redis(CON_CONF)
        self.task = TaskHelper(
            self.consumer_settings,
            redis_instance=redis_instance)
        self.job_manager = JobManager(self.task, job_class=BaseJob)
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

    # Job API Functions that aren't pure delegation to Redis

    def pause(self, _id: str, tenant: str = None) -> bool:
        return self.job_manager.pause_job(_id)

    def resume(self, _id, tenant: str = None) -> bool:
        return self.job_manager.resume_job(_id)

    def status(self, _id: Union[str, List[str]], tenant: str = None) -> List:
        if isinstance(_id, str):
            return [self.job_manager.get_job_status(_id)]
        else:
            return [self.job_manager.get_job_status(j_id) for j_id in _id]

    # Generic API Functions that aren't pure delegation to Redis

    def validate(self, job, _type=None, verbose=False, tenant=None):
        # consumer the tenant argument only because other methods need it
        _cls = type(self)._classes.get(_type)
        if not _cls:
            return {'error': f'un-handled type: {_type}'}
        if verbose:
            return _cls._validate_pretty(job)
        else:
            return _cls._validate(job)

    def dispatch(self, tenant=None, _type=None, operation=None, request=None):
        return self.job_manager.dispatch_resource_call(
            tenant,
            _type,
            operation,
            request
        )
