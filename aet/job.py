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

import enum
from time import sleep
from threading import Thread

from .logger import LOG


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
        self.value = 0
        self._thread = Thread(target=self._run)
        self._thread.start()

    def set_config(self, config, resource=None):
        LOG.debug(f'Job {self._id} got new config {config}')
        self.config = config
        if resource:
            self.resource = resource
        self.status = JobStatus.RECONFIGURE

    def _run(self):
        try:
            while self.status is not JobStatus.STOPPED:
                if self.status is JobStatus.PAUSED:
                    sleep(0.25)  # wait for the status to change
                    continue
                if self.status is JobStatus.RECONFIGURE:
                    # Take the new configuration into account
                    LOG.debug(f'Job {self._id} is using a new configuration.')
                    # Ok, all done and back to normal.
                    self.status = JobStatus.NORMAL
                    continue
                # Do something useful here
                self.value += 1
                sleep(0.25)
            LOG.debug(f'Job {self._id} stopped normally.')
        except Exception as fatal:
            LOG.critical(f'job {self._id} failed with critical error {fatal}')

    def stop(self, *args, **kwargs):
        self.status = JobStatus.STOPPED


class JobManager(object):

    _job_redis_name = '_job:'
    _job_redis_path = f'{_job_redis_name}*'

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
        jobs = self.task.list(type='job')
        for job in jobs:
            self._init_job(job)
        self.listen_for_job_change()

    # Job Management

    def _init_job(self, job):
        LOG.debug(f'initalizing job: {job}')
        _id = self._get_id(job)
        if _id in self.jobs.keys():
            LOG.debug('Job {_id} exists, updating')
            self._configure_job(job)
        else:
            self._start_job(job)

    def _start_job(self, job):
        LOG.debug(f'starting job: {job}')
        _id = self._get_id(job)
        self.jobs[_id] = self.job_class(_id)
        self._configure_job(job)

    def _configure_job(self, job):
        _id = self._get_id(job)
        data = job['data']
        LOG.debug(f'Configuring job {_id}')
        self.jobs[_id].set_config(data)

    def _pause_job(self, job):
        LOG.debug(f'pausing job: {job}')
        _id = self._get_id(job)
        if _id in self.jobs:
            self.jobs[_id].status = JobStatus.PAUSED
        else:
            LOG.debug(f'Could not find job {_id} to pause.')

    def _stop_job(self, job):
        LOG.debug(f'stopping job: {job}')
        _id = self._get_id(job)
        if _id in self.jobs:
            self.jobs[_id].stop()
        else:
            LOG.debug(f'Could not find job {_id} to stop.')

    # Job Listening

    def listen_for_job_change(self):
        _path = type(self)._job_redis_path
        LOG.debug(f'Registering Job Change Listener, {_path}')
        self.task.subscribe(self.on_job_change, _path)

    def on_job_change(self, job):
        LOG.debug(f'Consumer received update on job: {job}')
        _type = job['type']
        if _type == 'set':
            self._init_job(job)

    # utility

    def _get_id(self, job):
        _id = job['id'].split(type(self)._job_redis_name)[1]
        LOG.debug(f'got {_id} from {job}')
        return _id

    def get_status(self, _type, _id):
        # There are likely different ways of getting the status for each type
        return 'unknown'
