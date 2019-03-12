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

from .api import APIServer
from .logger import LOG
from .task import TaskHelper
from .job import BaseJob

EXCLUDED_TOPICS = ['__confluent.support.metrics']


class BaseConsumer(object):

    _job_redis_path = '_job:*'
    _job_class = BaseJob

    def __init__(self, CON_CONF, KAFKA_CONF):
        self.consumer_settings = CON_CONF
        self.kafka_settings = KAFKA_CONF
        self.task = TaskHelper(self.consumer_settings)
        self.jobs = {}
        self._init_jobs()
        self.serve_api(self.consumer_settings)

    # Control API

    def serve_api(self, settings):
        self.api = APIServer(self, settings)
        self.api.serve()

    def stop(self, *args, **kwargs):
        LOG.info('Shutting down')
        self.api.stop()
        self.task.stop()
        LOG.info('Shutdown Complete')

    def load_schema(self, path=None):
        path = path if path else self.consumer_settings.get('schema_path')
        if not path:
            raise AttributeError('No schema path available for validations.')
        with open(path) as f:
            return json.load(f)

    # Job Initialization

    def _init_jobs(self):
        jobs = self.task.list(type='job')
        for job in jobs:
            self._init_job(job)
        self.listen_for_job_change()

    # Job Management

    def _init_job(self, job):
        LOG.debug(f'initalizing job: {job}')
        _id = job['id']
        if _id in self.jobs.keys():
            LOG.debug('Job {_id} exists, updating')
        else:
            self._start_job(job)

    def _start_job(self, job):
        LOG.debug(f'starting job: {job}')
        self.jobs[job['id']] = job

    def _pause_job(self, job):
        LOG.debug(f'pausing job: {job}')

    def _stop_job(self, job):
        LOG.debug(f'stopping job: {job}')

    # Job Listening

    def listen_for_job_change(self):
        LOG.debug('Registering Job Change Listener')
        self.task.subscribe(self.on_job_change, BaseConsumer._job_redis_path)

    def on_job_change(self, job):
        LOG.debug(f'Consumer received update on job: {job}')
        _type = job['type']
        if _type == 'set':
            self._init_job(job)

    # Job API Functions -> Delegate to Redis

    def validate_job(self, job, schema=None):
        schema = schema if schema else self.schema
        try:
            jsonschema.validate(job, schema)  # Throws ValidationErrors
            return True
        except jsonschema.exceptions.ValidationError as err:
            LOG.debug(err)
            return False

    def add_job(self, job):
        if not self.validate_job(job):
            return False
        return self.task.add(job, type='job')

    def job_exists(self, _id):
        return self.task.exists(_id, type='job')

    def remove_job(self, _id):
        return self.task.remove(_id, type='job')

    def get_job(self, _id):
        return json.loads(self.task.get(_id, type='job'))

    def list_jobs(self):
        status = {}
        for job_id in self.task.list(type='job'):
            if job_id in self.jobs:
                status[job_id] = str(self.jobs.get(job_id).status)
            else:
                status[job_id] = 'unknown'
        return status


if __name__ == '__main__':
    from .settings import CONSUMER_SETTINGS, KAFKA_SETTINGS
    consumer = BaseConsumer(CONSUMER_SETTINGS, KAFKA_SETTINGS)
    consumer.serve_api(consumer.consumer_settings)
