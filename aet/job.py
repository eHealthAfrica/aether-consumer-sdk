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

from copy import deepcopy
import enum
from functools import partialmethod
from time import sleep
from threading import Thread
from typing import Dict, Optional

from .logger import LOG
from .jsonpath import CachedParser
from .task import Task


class JobStatus(enum.Enum):
    STOPPED = 0
    DEAD = 1
    PAUSED = 2  # Paused
    RECONFIGURE = 3  # Job has a new configuration
    NORMAL = 4  # Topic is operating normally


class BaseJob(object):
    def __init__(self, _id):
        self._id = _id
        self.status = JobStatus.PAUSED
        self.config = None
        self.resources = {}
        self.value = 0
        self._start()

    def set_config(self, config):
        LOG.debug(f'Job {self._id} got new config {config}')
        self.config = config
        self.status = JobStatus.RECONFIGURE

    def set_resource(self, resource, _type):
        self.resouces[_type] = resource
        self.status = JobStatus.RECONFIGURE

    def _run(self):
        try:
            while self.status is not JobStatus.STOPPED:
                if self.status is JobStatus.PAUSED:
                    sleep(0.25)  # wait for the status to change
                    continue
                if self.status is JobStatus.RECONFIGURE:
                    # Take the new configuration into account if anything needs to happen
                    # before the work part of the cycles. New DB connection etc.
                    self._handle_new_settings()
                    LOG.debug(f'Job {self._id} is using a new configuration.')
                    # Ok, all done and back to normal.
                    self.status = JobStatus.NORMAL
                    continue
                # Do something useful here
                # get a deepcopy of config & resources so they don't mutate mid-process
                _config, _resources = self._copy_settings()
                messages = self._get_messages(_config, _resources)
                if messages:
                    self._handle_messages(_config, _resources)
            LOG.debug(f'Job {self._id} stopped normally.')
        except Exception as fatal:
            LOG.critical(f'job {self._id} failed with critical error {fatal}')
            self.status = JobStatus.DEAD

    def _handle_new_settings(self):
        # block and handle changes to self.config or self.resources
        pass

    def _copy_settings(self):
        return deepcopy(self.config), deepcopy(self.resources)

    def _get_messages(self, config, resources):
        # probably needs custom implementation for each consumer
        return [1, 2]  # get from Kafka or...

    def _handle_messages(self, config, resources):
        # probably needs custom implementation for each consumer
        # Do something based on the messages
        self.value += 1

    def _start(self):
        self.status = JobStatus.NORMAL
        self._thread = Thread(target=self._run)
        self._thread.start()

    def stop(self, *args, **kwargs):
        self.status = JobStatus.STOPPED


class JobManager(object):

    _job_redis_type = 'job'
    _job_redis_name = f'_{_job_redis_type}:'
    _job_redis_path = f'{_job_redis_name}*'
    # Any type here needs to be registered in the API as APIServer._allowed_types
    _resources = {
        'resource': {
            'redis_type': 'resource',
            'redis_name': '_resource:',
            'redis_path': '_resource:*',  # Where to subscribe for this type in Redis
            'job_path': '$.resources'  # Where to find the resource reference in the job
        }
    }

    def __init__(self, task_master, job_class=BaseJob):
        self.task = task_master
        self.job_class = job_class
        self.jobs = {}
        self.resources = {}
        self._init_jobs()

    def stop(self, *args, **kwargs):
        for _id, job in self.jobs.items():
            LOG.debug(f'Stopping job {_id}')
            job.stop()

    # Job Initialization

    def _init_jobs(self):
        jobs = self.task.list(type=type(self)._job_redis_type)
        for _id in jobs:
            job = self.task.get(_id, type=type(self)._job_redis_type)
            LOG.debug(f'init job: {job}')
            self._init_job(job)
        self.listen_for_job_change()

    # Job Management

    def _init_job(self, job: dict) -> None:
        LOG.debug(f'initalizing job: {job}')
        _id = job['id']
        if _id in self.jobs.keys():
            LOG.debug('Job {_id} exists, updating')
            self._configure_job(job)
        else:
            self._start_job(job)

    def _start_job(self, job: dict) -> None:
        LOG.debug(f'starting job: {job}')
        _id = self._get_id(job=job)
        self.jobs[_id] = self.job_class(_id)
        self._configure_job(job)

    def _configure_job(self, job: dict):
        _id = self._get_id(job=job)
        LOG.debug(f'Configuring job {_id}')
        try:
            self._configure_job_resources(job)
        except AttributeError as aer:
            LOG.critical(f'Job {_id} missing required resource, stopping: {aer}')
            self.jobs[_id].stop()
            return
        self.jobs[_id].set_config(job)

    def _configure_job_resources(self, job: dict) -> None:
        job_id = job['id']
        type_paths = [(k, r['job_path']) for k, r in type(self)._resources.items()]
        for _type, path in type_paths:
            matches = CachedParser.find(path, job)
            if not matches:
                return  # no dependant resource of this type
            resource_id = [m.value for m in matches][0]
            LOG.debug(f'Job : {job_id} depends on resource {_type}:{resource_id}')
            self._register_resource_listener(
                _type,
                resource_id,
                self.jobs[job_id]
            )

    def _pause_job(self, _id: str) -> None:
        LOG.debug(f'pausing job: {_id}')
        if _id in self.jobs:
            self.jobs[_id].status = JobStatus.PAUSED
        else:
            LOG.debug(f'Could not find job {_id} to pause.')

    def _stop_job(self, _id: str) -> None:
        LOG.debug(f'stopping job: {_id}')
        if _id in self.jobs:
            self.jobs[_id].stop()
        else:
            LOG.debug(f'Could not find job {_id} to stop.')

    def _remove_job(self, _id: str) -> None:
        LOG.debug(f'removing job: {_id}')
        self._stop_job(_id)
        del self.jobs[_id]

    # Job Listening

    def listen_for_job_change(self):
        _path = type(self)._job_redis_path
        LOG.debug(f'Registering Job Change Listener, {_path}')
        self.task.subscribe(self.on_job_change, _path)

    def listen_for_resource_change(self):
        for _type, rule in type(self)._resources.items():
            redis_path = rule['redis_path']
            LOG.debug(f'Listening for resource {_type} on {redis_path}')
            # use a partial to keep from having to inspect the message source later
            callback = partialmethod(self.on_resource_change, _type)
            self.task.subscribe(callback, redis_path)

    def _register_resource_listener(
        self,
        _type: str,
        resource_id: str,
        job: BaseJob
    ) -> None:

        # register callbacks for job resource listeners
        # make sure structures exist for this _type & resource_id
        self.resources[_type] = self.resources.get(_type, {})
        self.resources[_type][resource_id] = self.resources[_type].get(resource_id, [])
        # see if this job is already listening to this resource to avoid duplicates
        listening_jobs = self.resources[_type][resource_id]
        if job._id not in [j._id for j in listening_jobs]:
            LOG.debug(f'Job {job._id} already subscribed to {_type} : {resource_id}')
            return
        # add job reference to listening jobs
        listening_jobs.append(job)
        # TODO KILL kill reference check
        assert(job._id in [j._id for j in self.resources[_type][resource_id]])

    def on_resource_change(
        self,
        _type: str,
        msg: Task
    ) -> None:

        # find out which jobs this pertains to
        LOG.debug(f'Consumer resource update : "{_type}": {msg}')
        resource_id = msg.id.split(self.resources[_type]['redis_name'])[1]
        # find jobs this pertains to
        jobs = self.resources[_type][resource_id]
        for job in jobs:
            if msg.type == 'set':
                LOG.debug(f'Sending update to subscriber {job._id}')
                job.set_resource(msg.data)
            elif msg.type == 'del':
                LOG.debug(f'Job: {job._id} has unmet resource dependency'
                          f' {_type}:{msg.id}. Stopping.')
                job.stop()

    def on_job_change(self, msg: Task) -> None:
        LOG.debug(f'Consumer received cmd: "{msg.type}" on job: {msg.id}')
        if msg.type == 'set':
            job = msg.data
            self._init_job(job)
        elif msg.type == 'del':
            _id = self._get_id(msg)  # just the id
            self._remove_job(_id)

    # utility

    def _get_id(self, task: Optional[Task] = None, job: Optional[Dict] = None):
        # from a signal
        if task:
            _id = task.id.split(type(self)._job_redis_name)[1]
        elif job:
            _id = job['id']
        return _id

    def get_status(self, _type, _id):
        # There are likely different ways of getting the status for each type
        return 'unknown'
