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
from functools import partial
from time import sleep
from threading import Thread
from typing import Any, Callable, ClassVar, Dict, Union

from aether.python.redis.task import Task, TaskHelper

from .logger import get_logger
from .jsonpath import CachedParser
from .resource import BaseResource

LOG = get_logger('Job')


class JobStatus(enum.Enum):
    STOPPED = 0
    DEAD = 1
    PAUSED = 2  # Paused
    RECONFIGURE = 3  # Job has a new configuration
    NORMAL = 4  # Job is operating normally


class BaseJob(object):

    _id: str
    status: JobStatus = JobStatus.PAUSED
    config: dict = {}
    resources: dict = {}
    value: int = 0
    # loop pause delay
    sleep_delay: float = 0.01
    # debug reporting interval
    report_interval: int = 10
    # join timeout for the worker thread
    shutdown_grace_period: int = 5

    def __init__(self, _id):
        self._id = _id
        self._start()

    def set_config(self, config: dict) -> None:
        LOG.debug(f'Job {self._id} got new config {config}')
        self.config = config
        if self.status is JobStatus.STOPPED:
            self._start()
        else:
            self.status = JobStatus.RECONFIGURE

    def set_resource(self, _type: str, resource: dict) -> None:
        LOG.debug(f'Job {self._id} got updated resource:  {resource}')
        self.resources[_type] = resource
        if self.status is JobStatus.STOPPED:
            self._start()
        else:
            self.status = JobStatus.RECONFIGURE

    def _run(self):
        try:
            c = 0
            while self.status is not JobStatus.STOPPED:
                c += 1
                if c % self.report_interval == 0:
                    LOG.debug(f'thread {self._id} running : {self.status}')
                if self.status is JobStatus.PAUSED:
                    sleep(self.sleep_delay)  # wait for the status to change
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
                    self._handle_messages(_config, _resources, messages)
            LOG.debug(f'Job {self._id} stopped normally.')
        except Exception as fatal:
            LOG.critical(f'job {self._id} failed with critical error {fatal}')
            self.status = JobStatus.DEAD

    def _handle_new_settings(self):
        # blocks and handles changes to self.config or self.resources
        pass

    def _copy_settings(self):
        return deepcopy(self.config), deepcopy(self.resources)

    def _get_messages(self, config, resources):
        # probably needs custom implementation for each consumer
        return [1, 2]  # get from Kafka or...

    def _handle_messages(self, config, resources, messages):
        # probably needs custom implementation for each consumer
        # Do something based on the messages
        sleep(self.sleep_delay)
        self.value += 1

    def _cause_exception(self) -> None:
        # intentionally cause the thread to crash for testing purposes
        # should yield status.DEAD and throw a critical message for TypeError
        self.value = None  # type: ignore

    def _start(self):
        LOG.debug(f'Job {self._id} starting')
        self.status = JobStatus.NORMAL
        self._thread = Thread(target=self._run)
        self._thread.start()

    def get_status(self) -> Union[Dict[str, Any], str]:
        # externally available information about this job.
        # At a minimum should return the running status
        return str(self.status)

    def stop(self, *args, **kwargs):
        self.status = JobStatus.STOPPED
        self._thread.join(self.shutdown_grace_period)


class JobManager(object):

    _job_redis_type: ClassVar[str] = 'job'
    _job_redis_name: ClassVar[str] = f'_{_job_redis_type}:'
    _job_redis_path: ClassVar[str] = f'{_job_redis_name}*'
    # Any type here needs to be registered in the API as APIServer._allowed_types
    _resources: ClassVar[dict] = {
        'resource': {
            'redis_type': 'resource',
            'redis_name': '_resource:',
            'redis_path': '_resource:*',  # Where to subscribe for this type in Redis
            'job_path': '$.resources',  # Where to find the resource reference in the job
            'class': BaseResource       # Subclass of BaseResource
        }
    }

    jobs: Dict[str, BaseJob] = {}
    resources: Dict[str, dict] = {}
    task: TaskHelper
    job_class: Callable

    # Start / Stop

    def __init__(self, task_master: TaskHelper, job_class: Callable = BaseJob):
        self.task = task_master
        self.job_class = job_class  # type: ignore
        self._init_jobs()

    def stop(self, *args, **kwargs):
        for _id, job in self.jobs.items():
            LOG.debug(f'Stopping job {_id}')
            job.stop()

    # Job Initialization

    def _init_jobs(self):
        jobs = self.task.list(type=type(self)._job_redis_type)
        for _id in jobs:
            job: Dict = self.task.get(_id, type=type(self)._job_redis_type)
            LOG.debug(f'init job: {job}')
            self._init_job(job)
        self.listen_for_job_change()
        self.listen_for_resource_change()

    # Job Management, driven by Redis or other Indirect Events (Startup/ shutdown etc)

    def _init_job(
        self,
        job: Dict[str, Any]
    ) -> None:
        # Attempt to start or update a job
        LOG.debug(f'initalizing job: {job}')
        _id = job['id']
        if _id in self.jobs.keys():
            LOG.debug(f'Job {_id} exists, updating')
            self._configure_job(job)
        else:
            self._start_job(job)

    def _start_job(
        self,
        job: Dict[str, Any]
    ) -> None:
        # Start a new job
        LOG.debug(f'starting job: {job}')
        _id = job['id']
        self.jobs[_id] = self.job_class(_id)
        self._configure_job(job)

    def _configure_job(
        self,
        job: Dict[str, Any]
    ):
        # Configure an existing job
        _id = job['id']
        LOG.debug(f'Configuring job {_id}')
        try:
            self._configure_job_resources(job)
            LOG.debug(f'Resources configured for {_id}')
        except (
            AttributeError,
            ValueError
        ) as aer:
            LOG.error(f'Job {_id} missing required resource, stopping: {aer}')
            self._stop_job(_id)
            return
        self.jobs[_id].set_config(job)

    def _configure_job_resources(
        self,
        job: Dict[str, Any]
    ) -> None:
        # Configure resources for a job
        job_id = job['id']
        type_paths = [(k, r['job_path']) for k, r in type(self)._resources.items()]
        LOG.debug(f'Job: {job_id} triggered checks on paths {type_paths}')
        added_resources = []
        for _type, path in type_paths:
            matches = CachedParser.find(path, job)
            if not matches:
                LOG.debug(f'Job: {job_id} has no external resources for path {path}')
                continue  # no dependent resource of this type
            resource_id = [m.value for m in matches][0]
            LOG.debug(f'Job : {job_id} depends on resource {_type}:{resource_id}')
            added = self._register_resource_listener(
                _type,
                resource_id,
                self.jobs[job_id]
            )
            if added:
                added_resources.append([_type, resource_id])
        for _type, resource_id in added_resources:
            self._init_resource(_type, resource_id, self.jobs[job_id])

    def _stop_job(self, _id: str) -> None:
        LOG.debug(f'stopping job: {_id}')
        if _id in self.jobs:
            self.jobs[_id].stop()

    def _remove_job(self, _id: str) -> None:
        LOG.debug(f'removing job: {_id}')
        self._stop_job(_id)
        self._remove_resource_listeners(_id)
        del self.jobs[_id]

    # Direct API Driven job control / visibility functions

    def pause_job(self, _id: str) -> bool:
        LOG.debug(f'pausing job: {_id}')
        if _id in self.jobs:
            self.jobs[_id].status = JobStatus.PAUSED
            return True
        else:
            LOG.debug(f'Could not find job {_id} to pause.')
            return False

    def resume_job(self, _id: str) -> bool:
        LOG.debug(f'pausing job: {_id}')
        if _id in self.jobs:
            self.jobs[_id].status = JobStatus.NORMAL
            return True
        else:
            LOG.debug(f'Could not find job {_id} to pause.')
            return False

    def get_job_status(self, _id: str) -> Union[Dict[str, Any], str]:
        if _id in self.jobs:
            return self.jobs[_id].get_status()
        else:
            return f'no job with id:{_id}'

    def dispatch_resource_call(self, tenant=None, _type=None, operation=None, request=None):
        pass
        # _cls = type(self).get(_type, {}).get('class')
        # if operation in _cls.static_actions:
        #     fn = getattr(_cls, operation)

    #############
    #
    # Listening and Callbacks
    #
    #############

    # register generic listeners on a redis path

    def listen_for_job_change(self):
        _path = type(self)._job_redis_path
        LOG.debug(f'Registering Job Change Listener, {_path}')
        self.task.subscribe(self.on_job_change, _path)

    def listen_for_resource_change(self):
        # handles work for all resource types
        for _type, rule in type(self)._resources.items():
            redis_path = rule['redis_path']
            LOG.debug(f'Listening for resource {_type} on {redis_path}')
            # use a partial to keep from having to inspect the message source later
            callback = partial(self.on_resource_change, _type)
            self.task.subscribe(callback, redis_path)

    # attach a resource listener to a job that depends on it
    def _register_resource_listener(
        self,
        _type: str,
        resource_id: str,
        job: BaseJob
    ) -> bool:

        # register callbacks for job resource listeners
        # make sure structures exist for this _type & resource_id
        if not self.resources.get(_type):
            self.resources[_type] = {}
        if not self.resources[_type].get(resource_id):
            self.resources[_type][resource_id] = []
        # see if this job is already listening to this resource to avoid duplicates
        listening_jobs = self.resources[_type][resource_id]
        if job._id in [j._id for j in listening_jobs]:
            LOG.debug(f'Job {job._id} already subscribed to {_type} : {resource_id}')
            return False
        # add job reference to listening jobs
        LOG.debug(f'Job {job._id} subscribing to {_type} : {resource_id}')
        listening_jobs.append(job)
        return True

    # remove all resource listeners for shutdown
    def _remove_resource_listeners(self, job_id: str) -> None:
        # remove all resource listeners for a job with id -> job_id
        for _type in self.resources.keys():
            for resource_id in self.resources[_type].keys():
                jobs = [j for j in self.resources[_type][resource_id] if j._id != job_id]
                self.resources[_type][resource_id] = jobs
        return

    # manually go out and grab a resource // happens on startup
    def _init_resource(
        self,
        _type: str,
        resource_id: str,
        job: BaseJob
    ) -> None:
        LOG.debug(f'Initializing resource for job {job._id} -> {_type}:{resource_id}')
        res = self.task.get(_id=resource_id, type=_type)
        job.set_resource(_type, res)
        return

    # generic handler called on change of any resource. Dispatched to dependent jobs
    def on_resource_change(
        self,
        _type: str,
        msg: Task
    ) -> None:

        # find out which jobs this pertains to
        LOG.debug(f'Consumer resource update : "{_type}": {msg}')
        resource_id = msg.id.split(
            type(self)._resources[_type]['redis_name'])[1]
        # find jobs this pertains to
        jobs = self.resources.get(_type, {}).get(resource_id, {})
        if not jobs:
            LOG.debug(f'No matching jobs for resource {_type}:{resource_id} registered. Ignoring')
            return
        job: BaseJob
        for job in jobs:
            if msg.type == 'set':
                LOG.debug(f'Sending update to subscriber {job._id}')
                job.set_resource(_type, msg.data)  # type: ignore  # overly pedantic with dicts
            elif msg.type == 'del':
                LOG.debug(f'Job: {job._id} has unmet resource dependency'
                          f' {_type}:{msg.id}. Stopping.')
                job.stop()

    # generic job change listener then dispatched to the proper job / creates new job
    def on_job_change(self, msg: Task) -> None:
        LOG.debug(f'Consumer received cmd: "{msg.type}" on job: {msg.id}')
        if msg.type == 'set':
            job = msg.data
            if job:  # type checker gets mad without the check here
                self._init_job(job)
        elif msg.type == 'del':
            _id = self._get_id(msg)  # just the id
            self._remove_job(_id)

    #############
    #
    # Utility
    #
    #############

    def _get_id(
        self,
        msg: Task,
    ) -> str:
        # from a signal
        return msg.id.split(type(self)._job_redis_name)[1]
