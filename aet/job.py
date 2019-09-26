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
import json
from time import sleep
from threading import Thread
from typing import Any, Callable, ClassVar, Dict, List, Union

from aether.python.redis.task import Task, TaskEvent, TaskHelper
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from .jsonpath import CachedParser
from .logger import get_logger
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

    resources: InstanceManager
    value: int = 0
    # loop pause delay
    sleep_delay: float = 0.01
    # debug reporting interval
    report_interval: int = 10
    # join timeout for the worker thread
    shutdown_grace_period: int = 5
    # the configuration schema for instances of this job
    schema: str = None  # jsonschema for job instructions
    tenant: str  # tenant for this job
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

    @classmethod
    def get_schema(cls):
        return cls.schema

    def __init__(self, _id: str, tenant: str, resources: InstanceManager):
        self._id = _id
        self.tenant = tenant
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
                    self.safe_sleep(self.sleep_delay)  # wait for the status to change
                    continue
                if self.status is JobStatus.RECONFIGURE:
                    # Take the new configuration into account if anything needs to happen
                    # before the work part of the cycles. New DB connection etc.
                    self._handle_new_settings()
                    LOG.debug(f'Job {self._id} is using a new configuration. {self.config}')
                    # Ok, all done and back to normal.
                    self.status = JobStatus.NORMAL
                    continue
                # Do something useful here
                # get a deepcopy of config
                config = deepcopy(self.config)
                if not config:
                    # config changed in flight, try again
                    continue
                try:
                    LOG.debug(f'{self._id} -> {self.status}')
                    messages = self._get_messages(config)
                    if messages:
                        self._handle_messages(config, messages)
                except RuntimeError as rer:
                    LOG.error(f'{self._id} : {rer}')
                    self.safe_sleep(10)
            LOG.debug(f'Job {self._id} stopped normally.')
        except Exception as fatal:
            LOG.critical(f'job {self._id} failed with critical error {fatal}')
            self.status = JobStatus.DEAD

    def _get_messages(self, config):
        # probably needs custom implementation for each consumer
        return [1, 2]  # get from Kafka or...

    def _handle_new_settings(self):
        pass

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
        self._thread = Thread(target=self._run, daemon=True)
        self._thread.start()

    def get_status(self) -> Union[Dict[str, Any], str]:
        # externally available information about this job.
        # At a minimum should return the running status
        return str(self.status)

    def get_resources(self, _type, config) -> List[BaseResource]:
        path = type(self)._resources.get(_type, {}).get('job_path')
        matches = CachedParser.find(path, config)
        if not matches:
            LOG.debug(f'{self._id} found no resources in {matches}, {path} -> {config}')
            return []
        resource_ids = [m.value for m in matches][0]
        LOG.debug(f'{self._id} found resource ids {resource_ids}')
        resources = []
        for _id in resource_ids:
            res = self.get_resource(_type, _id)
            if res:
                resources.append(res)
            else:
                LOG.critical(f'in {self._id} resource {_id} not available')
        return resources

    def get_resource(self, _type, _id) -> BaseResource:
        return self.resources.get(_id, _type, self.tenant)

    def safe_sleep(self, dur):
        for x in range(dur):
            if self.status == JobStatus.STOPPED:
                break
            sleep(1)

    def stop(self, *args, **kwargs):
        # return thread to be observed
        LOG.info(f'Job {self._id} caught stop signal.')
        self.status = JobStatus.STOPPED
        return self._thread


class JobManager(object):

    jobs: Dict[str, BaseJob]
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
        self.jobs = {}
        self.task = task_master
        self.job_class = job_class  # type: ignore
        self.resources = InstanceManager(self.job_class._resources)
        self._init_resources()
        self._init_jobs()
        LOG.debug('JobManager Ready')

    def stop(self, *args, **kwargs):
        threads = []
        for _id, job in self.jobs.items():
            threads.append(job.stop())
        LOG.info('Stopping Job Threads...')
        [t.join() for t in threads]
        LOG.info('Stopping Resources...')
        [t.join() for t in self.resources.stop()]

    # Job Initialization

    def _init_jobs(self):
        jobs = list(self.task.list(self.job_class._job_redis_type))
        for job in jobs:
            tenant, _id = job.split(':')
            job: Dict = self.task.get(_id, type=self.job_class._job_redis_type, tenant=tenant)
            LOG.debug(f'init job: {job}')
            self._init_job(job, tenant)
        self.listen_for_job_change()

    def _init_resources(self):
        resources = self.job_class._resources
        for _type, rule in resources.items():
            _type = rule['redis_type']
            keys = [k.split(':') for k in self.task.list(_type)]
            for tenant, _id in keys:
                LOG.debug(f'Init resource: {tenant}:{_id}')
                body = self.task.get(_id, _type, tenant)
                self.resources.update(_id, _type, tenant, body)
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
            self.jobs[_id].set_config(job)
        else:
            LOG.debug(f'Creating new job {_id}')
            self.jobs[_id] = self.job_class(_id, tenant, self.resources)
            self.jobs[_id].set_config(job)

    def list_jobs(self, tenant: str):
        job_ids = list(self.jobs.keys())
        return [_id.split(':')[1] for _id in job_ids]

    def remove_job(self, _id: str, tenant: str) -> None:
        job_id = JobManager.get_job_id(_id, tenant)
        LOG.debug(f'removing job: {job_id}')
        job_ids = list(self.jobs.keys())
        if job_id in job_ids:
            self.jobs[job_id].stop()
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
        LOG.debug(f'resuming job: {job_id}')
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
        _path = self.job_class._job_redis_path
        LOG.debug(f'Registering Job Change Listener, {_path}')
        self.task.subscribe(self.on_job_change, _path, True)

    def listen_for_resource_change(self):
        # handles work for all resource types
        resources = self.job_class._resources
        for _type, rule in resources.items():
            redis_path = rule['redis_path']
            LOG.debug(f'Listening for resource {_type} on {redis_path}')
            # use a partial to keep from having to inspect the message source later
            self.task.subscribe(self.on_resource_change, redis_path, True)

    # generic handler called on change of any resource. Dispatched to dependent jobs
    def on_resource_change(
        self,
        msg: Union[Task, TaskEvent]
    ) -> None:
        self.resources.on_resource_change(msg)

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
                self.remove_job(msg.task_id, msg.tenant)
