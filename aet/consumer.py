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

import json
import jsonschema
from time import sleep
from typing import Any, Dict, List, Union

from .api import APIServer
from .logger import LOG
from .task import TaskHelper
from .job import JobManager, BaseJob
from .settings import Settings

EXCLUDED_TOPICS = ['__confluent.support.metrics']


class BaseConsumer(object):

    api: APIServer
    consumer_settings: Settings
    kafka_settings: Settings
    job_manager: JobManager
    schemas: Dict[str, Any] = {}
    task: TaskHelper

    def __init__(self, CON_CONF, KAFKA_CONF):
        self.consumer_settings = CON_CONF
        self.kafka_settings = KAFKA_CONF
        self.task = TaskHelper(self.consumer_settings)
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

    def load_schema(self, path=None):
        path = path if path else self.consumer_settings.get('schema_path')
        if not path:
            raise AttributeError('No schema path available for validations.')
        with open(path) as f:
            return json.load(f)

    # Job API Functions that aren't pure delegation to Redis

    def pause(self, _id: str) -> bool:
        return self.job_manager.pause_job(_id)

    def resume(self, _id) -> bool:
        return self.job_manager.resume_job(_id)

    def status(self, _id: Union[str, List[str]]) -> List:
        if isinstance(_id, str):
            return [self.job_manager.get_job_status(_id)]
        else:
            return [self.job_manager.get_job_status(j_id) for j_id in _id]

    # Generic API Functions that aren't pure delegation to Redis

    def validate(self, job, _type=None, schema=None):
        schema = schema if schema else self.schemas.get(_type, {})
        try:
            jsonschema.validate(job, schema)  # Throws ValidationErrors
            return True
        except jsonschema.exceptions.ValidationError as err:
            LOG.debug(err)
            return False
