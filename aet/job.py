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
import json
from time import sleep
from threading import Thread
from typing import Any, Callable, ClassVar, Dict, Union

from aether.python.redis.task import Task, TaskEvent, TaskHelper
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from .logger import get_logger
from .jsonpath import CachedParser
from .resource import BaseResource, InstanceManager

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
    resources: InstanceManager
    value: int = 0
    # loop pause delay
    sleep_delay: float = 0.01
    # debug reporting interval
    report_interval: int = 10
    # join timeout for the worker thread
    shutdown_grace_period: int = 5
    # the configuration schema for instances of this job
    schema: str  # jsonschema for job instructions
    validator: Any = None  # jsonschema validation object

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

    def __init__(self, _id: str, tenant: str, resources: InstanceManager):
        self._id = _id
        self.tenant = tenant,
        self.resources = resources
        self._start()

    def set_config(self, config: dict) -> None:
        LOG.debug(f'Job {self._id} got new config {config}')
        self.config = config
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
                # get a deepcopy of config
                config = deepcopy(self.config)
                messages = self._get_messages(config)
                if messages:
                    self._handle_messages(config, messages)
            LOG.debug(f'Job {self._id} stopped normally.')
        except Exception as fatal:
            LOG.critical(f'job {self._id} failed with critical error {fatal}')
            self.status = JobStatus.DEAD

    def _handle_new_settings(self):
        # blocks and handles changes to self.config or self.resources
        pass

    def _get_messages(self, config):
        # probably needs custom implementation for each consumer
        return [1, 2]  # get from Kafka or...

    def _handle_messages(self, config, messages):
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

    def get_resource(self, _type, _id):
        return self.resources.get(_id, _type, self.tenant)

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
    resources: InstanceManager
    task: TaskHelper
    job_class: Callable

    @staticmethod
    def get_job_id(job: Union[str, Dict[str, Any]], tenant: str):
        if isinstance(job, dict):
            _id = job.get('id')
        else:
            _id = job
        return f'{tenant}:{_id}'

    # Start / Stop

    def __init__(self, task_master: TaskHelper, job_class: Callable = BaseJob):
        self.task = task_master
        self.job_class = job_class  # type: ignore
        self.resource = InstanceManager(type(self)._resources)
        self._init_jobs()

    def stop(self, *args, **kwargs):
        for _id, job in self.jobs.items():
            LOG.debug(f'Stopping job {_id}')
            job.stop()

    # Job Initialization

    def _init_jobs(self):
        jobs = self.task.list(type=type(self)._job_redis_type, tenant='*')

        LOG.debug(f'jobs: {jobs}')
        for job in jobs:
            tenant, _type, _id = job.split(':')
            job: Dict = self.task.get(_id, type=type(self)._job_redis_type, tenant=tenant)
            LOG.debug(f'init job: {job}')
            self._init_job(job)
        self.listen_for_job_change()
        self.listen_for_resource_change()

    # Job Management, driven by Redis or other Indirect Events (Startup/ shutdown etc)

    def _init_job(
        self,
        job: Dict[str, Any],
        tenant: str
    ) -> None:
        # Attempt to start or update a job
        LOG.debug(f'initalizing job: {job}')
        _id = JobManager.get_job_id(job, tenant)
        if _id in self.jobs.keys():
            LOG.debug(f'Job {_id} exists, updating')
            self._configure_job(job, tenant)
        else:
            self._start_job(job, tenant)

    def _start_job(
        self,
        job: Dict[str, Any],
        tenant: str
    ) -> None:
        # Start a new job
        LOG.debug(f'starting job: {job}')
        _id = JobManager.get_job_id(job, tenant)
        self.jobs[_id] = self.job_class(_id, self.resources)
        self.jobs[_id].set_config(job)

    # def _configure_job(
    #     self,
    #     job: Dict[str, Any],
    #     tenant: str
    # ):
    #     # Configure an existing job
    #     job_id = JobManager.get_job_id(job, tenant)
    #     self.jobs[job_id].set_config(job)

    # def _configure_job_resources(
    #     self,
    #     job: Dict[str, Any]
    # ) -> None:
    #     # Configure resources for a job
    #     job_id = job['id']
    #     type_paths = [(k, r['job_path']) for k, r in type(self)._resources.items()]
    #     LOG.debug(f'Job: {job_id} triggered checks on paths {type_paths}')
    #     added_resources = []
    #     for _type, path in type_paths:
    #         matches = CachedParser.find(path, job)
    #         if not matches:
    #             LOG.debug(f'Job: {job_id} has no external resources for path {path}')
    #             continue  # no dependent resource of this type
    #         resource_id = [m.value for m in matches][0]
    #         LOG.debug(f'Job : {job_id} depends on resource {_type}:{resource_id}')
    #         added = self._register_resource_listener(
    #             _type,
    #             resource_id,
    #             self.jobs[job_id]
    #         )
    #         if added:
    #             added_resources.append([_type, resource_id])
    #     for _type, resource_id in added_resources:
    #         self._init_resource(_type, resource_id, self.jobs[job_id])

    def _stop_job(self, _id: str, tenant: str) -> None:
        job_id = JobManager.get_job_id(_id, tenant)
        LOG.debug(f'stopping job: {job_id}')
        if job_id in self.jobs:
            self.jobs[job_id].stop()

    def _remove_job(self, _id: str, tenant: str) -> None:
        job_id = JobManager.get_job_id(_id, tenant)
        LOG.debug(f'removing job: {job_id}')
        self._stop_job(job_id, tenant)
        self._remove_resource_listeners(job_id)
        del self.jobs[job_id]

    # Direct API Driven job control / visibility functions

    def pause_job(self, _id: str, tenant: str) -> bool:
        job_id = JobManager.get_job_id(_id, tenant)
        LOG.debug(f'pausing job: {job_id}')
        if job_id in self.jobs:
            self.jobs[job_id].status = JobStatus.PAUSED
            return True
        else:
            LOG.debug(f'Could not find job {job_id} to pause.')
            return False

    def resume_job(self, _id: str, tenant: str) -> bool:
        job_id = JobManager.get_job_id(_id, tenant)
        LOG.debug(f'pausing job: {job_id}')
        if job_id in self.jobs:
            self.jobs[job_id].status = JobStatus.NORMAL
            return True
        else:
            LOG.debug(f'Could not find job {job_id} to pause.')
            return False

    def get_job_status(self, _id: str, tenant: str) -> Union[Dict[str, Any], str]:
        job_id = JobManager.get_job_id(_id, tenant)
        if job_id in self.jobs:
            return self.jobs[job_id].get_status()
        else:
            return f'no job with id:{job_id}'

    def dispatch_resource_call(self, tenant=None, _type=None, operation=None, request=None):
        return self.resources.dispatch(tenant, _type, operation, request)

    #############
    #
    # Listening and Callbacks
    #
    #############

    # register generic listeners on a redis path

    def listen_for_job_change(self):
        _path = type(self)._job_redis_path
        LOG.debug(f'Registering Job Change Listener, {_path}')
        self.task.subscribe(self.on_job_change, _path, True)

    def listen_for_resource_change(self):
        # handles work for all resource types
        for _type, rule in type(self)._resources.items():
            redis_path = rule['redis_path']
            LOG.debug(f'Listening for resource {_type} on {redis_path}')
            # use a partial to keep from having to inspect the message source later
            callback = partial(self.on_resource_change, _type)
            self.task.subscribe(callback, redis_path, True)

    # # attach a resource listener to a job that depends on it
    # def _register_resource_listener(
    #     self,
    #     _type: str,
    #     resource_id: str,
    #     job: BaseJob
    # ) -> bool:

    #     # register callbacks for job resource listeners
    #     # make sure structures exist for this _type & resource_id
        
    #     if not self.resources.get(_type):
    #         self.resources[_type] = {}
    #     if not self.resources[_type].get(resource_id):
    #         self.resources[_type][resource_id] = []
        
    #     # see if this job is already listening to this resource to avoid duplicates
    #     listening_jobs = self.resources[_type][resource_id]
    #     if job._id in [j._id for j in listening_jobs]:
    #         LOG.debug(f'Job {job._id} already subscribed to {_type} : {resource_id}')
    #         return False
    #     # add job reference to listening jobs
    #     LOG.debug(f'Job {job._id} subscribing to {_type} : {resource_id}')
    #     listening_jobs.append(job)
    #     return True

    # # remove all resource listeners for shutdown
    # def _remove_resource_listeners(self, job_id: str) -> None:
    #     # remove all resource listeners for a job with id -> job_id
    #     for _type in self.resources.keys():
    #         for resource_id in self.resources[_type].keys():
    #             jobs = [j for j in self.resources[_type][resource_id] if j._id != job_id]
    #             self.resources[_type][resource_id] = jobs
    #     return

    # manually go out and grab a resource // happens on startup
    # def _init_resource(
    #     self,
    #     _type: str,
    #     resource_id: str,
    #     job: BaseJob
    # ) -> None:
    #     LOG.debug(f'Initializing resource for job {job._id} -> {_type}:{resource_id}')
    #     res = self.task.get(_id=resource_id, type=_type)
    #     job.set_resource(_type, res)
    #     return

    def initialize_resources(self):
        for _type, rule in type(self)._resources.items():
            _type = rule['redis_type']
            self.task.list(_type, '*')
            


    # generic handler called on change of any resource. Dispatched to dependent jobs
    def on_resource_change(
        self,
        msg: Union[Task, TaskEvent]
    ) -> None:
        self.resources.on_resource_change(msg)

        # # find out which jobs this pertains to
        # LOG.debug(f'Consumer resource update : "{_type}": {msg}')
        # if isinstance(msg, Task):
        #     resource_id = msg.id
        # else:
        #     resource_id.event_id
        # # find jobs this pertains to
        # jobs = self.resources.get(_type, {}).get(resource_id, {})
        # if not jobs:
        #     LOG.debug(f'No matching jobs for resource {_type}:{resource_id} registered. Ignoring')
        #     return
        # job: BaseJob
        # for job in jobs:
        #     if isinstance(msg, Task):
        #         LOG.debug(f'Sending update to subscriber {job._id}')
        #         job.set_resource(_type, msg.data)  # type: ignore  # overly pedantic with dicts
        #     elif isinstance(msg, TaskEvent) and msg.event == 'del':
        #         LOG.debug(f'Job: {job._id} has unmet resource dependency'
        #                   f' {_type}:{msg.id}. Stopping.')
        #         job.stop()

    # generic job change listener then dispatched to the proper job / creates new job
    def on_job_change(self, msg: Union[Task, TaskEvent]) -> None:
        if isinstance(msg, Task):
            LOG.debug(f'Consumer received Task: "{msg.type}" on job: {msg.id}')
            job = msg.data
            tenant = msg.tenant
            if job:  # type checker gets mad without the check here
                self._init_job(job, tenant)
        elif isinstance(msg, TaskEvent):
            LOG.debug(f'Consumer received TaskEvent: "{msg}"')
            if msg.event == 'del':
                self._remove_job(msg.task_id)

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
