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

from abc import abstractmethod
from copy import deepcopy
from datetime import datetime
import enum
from time import sleep
from threading import Thread
import traceback
from typing import Any, Callable, Dict, List, Union

from aether.python.redis.task import Task, TaskEvent, TaskHelper

from .exceptions import ConsumerHttpException, MessageHandlingException
from .helpers import classproperty, require_property
from .jsonpath import CachedParser
from .logger import get_logger, callback_logger
from .resource import BaseResource, InstanceManager, ResourceReference, AbstractResource

LOG = get_logger('Job')


class JobStatus(enum.Enum):
    STOPPED = 0
    DEAD = 1
    PAUSED = 2  # Paused
    RECONFIGURE = 3  # Job has a new configuration
    NORMAL = 4  # Job is operating normally


class JobReference(object):
    redis_type: str
    redis_name: str
    redis_path: str

    def __init__(self, name):
        self.make_paths(name)

    def make_paths(self, name):
        name = name.lower()
        self.redis_type = f'{name}'
        self.redis_name = f'_{name}:'
        self.redis_path = f'{self.redis_name}*'


class BaseJob(AbstractResource):

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
    tenant: str  # tenant for this job
    validator: Any = None  # jsonschema validation object

    public_actions = AbstractResource.public_actions + [
        # These are only valid for jobs
        'pause',
        'resume',
        'get_status',
        'get_logs'
    ]
    _masked_fields: List[str] = []  # jsonpaths to be masked when showing definition

    @property
    @abstractmethod  # required
    def _resources(self) -> List[BaseResource]:
        return []

    @classproperty
    def reference(cls) -> JobReference:
        name = require_property(cls.name)
        return JobReference(name)

    def __init__(self, _id: str, tenant: str, resources: InstanceManager, context: 'JobManager'):
        self._id = _id
        self.tenant = tenant
        self.resources = resources
        self.context = context
        self.log_stack = []
        self.log = callback_logger(f'j-{self.tenant}-{self._id}', self.log_stack, 100)
        self._setup()
        self._start()

    # PUBLIC METHODS for job Management & Monitoring

    def pause(self, *args, **kwargs):
        '''
            Temporarily Pause a job execution.
            Will restart if the system resets. For a longer pause, remove the job via DELETE
        '''
        self.status = JobStatus.PAUSED
        return True

    def resume(self, *args, **kwargs):
        '''
            Resume the job after pausing it.
        '''
        if self.status is not JobStatus.DEAD:
            self.status = JobStatus.NORMAL
            return True
        else:
            self.log.info(f'Restarting dead job {self._id}')
            self._start()
            return True

    def get_status(self, *args, **kwargs) -> Union[Dict[str, Any], str]:
        # externally available information about this job.
        # At a minimum should return the running status
        return str(self.status)

    def get_logs(self, *arg, **kwargs):
        '''
        A list of the last 100 log entries from this job in format
        [
            (timestamp, log_level, message),
            (timestamp, log_level, message),
            ...
        ]
        '''
        return self.log_stack[:]

    def set_config(self, config: dict) -> None:
        self.log.debug(f'Job {self._id} got new config {config}')
        self.config = config
        if self.status is JobStatus.STOPPED:
            self._start()
        else:
            self.status = JobStatus.RECONFIGURE

    # Base Loop for all jobs

    def _run(self):
        try:
            c = 0
            while self.status is not JobStatus.STOPPED:
                self.context.check_in(self._id, datetime.now())
                c += 1
                if c % self.report_interval == 0:
                    self.log.debug(f'thread {self._id} running : {self.status}')
                if self.status is JobStatus.PAUSED:
                    self.safe_sleep(self.sleep_delay)  # wait for the status to change
                    continue
                if self.status is JobStatus.RECONFIGURE:
                    # Take the new configuration into account if anything needs to happen
                    # before the work part of the cycles. New DB connection etc.
                    self._handle_new_settings()
                    self.log.debug(f'Job {self._id} is using a new configuration. {self.config}')
                    # Ok, all done and back to normal.
                    self.status = JobStatus.NORMAL
                    continue
                # Do something useful here
                # get a deepcopy of config
                config = deepcopy(self.config)
                if not config:
                    # config changed in flight, try again
                    self.safe_sleep(self.sleep_delay)  # wait for the status to change
                    continue
                try:
                    self.log.debug(f'{self._id} -> {self.status}')
                    messages = self._get_messages(config)
                    if messages:
                        self._handle_messages(config, messages)
                except MessageHandlingException as mhe:
                    self._on_message_handle_exception(mhe)
                except RuntimeError as rer:
                    self.log.critical(f'RuntimeError: {self._id} | {rer}')
                    self.safe_sleep(self.sleep_delay)

            self.context.set_inactive(self._id)
            self.log.debug(f'Job {self._id} stopped normally.')
        except Exception as fatal:
            self.log.critical(f'job {self._id} failed with critical error {type(fatal)}: {fatal}')
            self.log.error(''.join(traceback.format_tb(fatal.__traceback__)))
            self.context.set_inactive(self._id)
            self.status = JobStatus.DEAD
            return  # we still want to be able to read the logs so don't re-raise

    @abstractmethod  # required
    def _get_messages(self, config):
        # probably needs custom implementation for each consumer
        return [1, 2]  # get from Kafka or...

    def _handle_new_settings(self):
        pass

    @abstractmethod  # required
    def _handle_messages(self, config, messages):
        # probably needs custom implementation for each consumer
        # Do something based on the messages
        sleep(self.sleep_delay)
        LOG.debug('Handling Messages')
        self.value += 1

    def _on_message_handle_exception(self, mhe: MessageHandlingException):
        pass

    def _cause_exception(self, exception: Exception = ValueError) -> None:
        # intentionally cause the thread to crash for testing purposes
        # should yield status.DEAD and throw a critical message for TypeError
        def _raise(self, *args, **kwargs):
            raise exception('TestException')
        self.log.debug(f'{self._id} throwing exception on next _handle_messages: {type(exception)}')
        self._handle_messages = _raise

    def _revert_exception(self):
        def _normal(self, *args, **kwargs):
            pass
        self.log.debug(f'{self._id} removing poison method')
        self._handle_messages = _normal

    def _setup(self):
        # runs before thread and allows for setup, we can also fault here if not
        # configured properly
        pass

    def _start(self):
        self.log.debug(f'Job {self._id} starting')
        self.status = JobStatus.NORMAL
        self._thread = Thread(target=self._run, daemon=True)
        self._thread.start()

    def get_resources(self, _type, config) -> List[BaseResource]:
        _cls = [_cls for _cls in self._resources if _cls.name == _type]
        if not _cls:
            return []
        _cls = _cls[0]
        path = _cls.reference.job_path
        matches = CachedParser.find(path, config)
        if not matches:
            return []
        resource_ids = [m.value for m in matches][0]
        self.log.debug(f'{self._id}: found {_type} ids:  {resource_ids}')
        resources = []
        if isinstance(resource_ids, str):
            resource_ids = [resource_ids]
        for _id in resource_ids:
            res = self.get_resource(_type, _id)
            if res:
                resources.append(res)
            else:
                self.log.critical(f'in {self._id} resource {_id} not available')
        return resources

    def get_resource(self, _type, _id) -> BaseResource:
        return self.resources.get(_id, _type, self.tenant)

    def safe_sleep(self, dur):
        if not isinstance(dur, int):
            sleep(dur)
            return
        for x in range(dur):
            if self.status == JobStatus.STOPPED:
                break
            sleep(1)

    def stop(self, *args, **kwargs):
        # return thread to be observed
        self.log.info(f'Job {self._id} caught stop signal.')
        self.context.set_inactive(self._id)
        self.status = JobStatus.STOPPED
        return self._thread


class JobManager(object):

    jobs: Dict[str, BaseJob]
    resources: InstanceManager
    task: TaskHelper

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
        self.check_ins = {}
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

    def set_inactive(self, _id):
        if _id in self.check_ins:
            del self.check_ins[_id]

    def check_in(self, _id, ts: datetime):
        self.check_ins[_id] = ts

    def status(self):
        _now = datetime.now()
        # if a job is inactive (stopped / paused intentionally or died naturally)
        # then it's remove from the check and given a value of _now
        idle_times = {_id: int((_now - self.check_ins.get(_id, _now)).total_seconds())
                      for _id in self.jobs.keys()}
        return idle_times

    # Job Initialization

    def _init_jobs(self):
        jobs = list(self.task.list(self.job_class.reference.redis_type))
        for job in jobs:
            tenant, _id = job.split(':')
            job: Dict = self.task.get(_id, type=self.job_class.reference.redis_type, tenant=tenant)
            LOG.debug(f'init job: {job}')
            self._init_job(job, tenant)
        self.listen_for_job_change()

    def _init_resources(self):
        for _cls in self.job_class._resources:
            ref: ResourceReference = _cls.reference
            _type = ref.redis_type
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
            self.jobs[_id] = self.job_class(_id, tenant, self.resources, self)
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

    def dispatch_job_call(
        self,
        tenant=None,
        _type=None,
        operation=None,
        _id=None,
        request=None
    ):
        LOG.debug(f'Dispatching Job request {_id} -> {tenant}:{_type}:{operation}')
        job_id = JobManager.get_job_id(_id, tenant)
        if job_id not in self.jobs:
            raise ConsumerHttpException(f'No resource of type "{_type}" with id "{_id}"', 404)
        inst = self.jobs[job_id]
        try:
            fn = getattr(inst, operation)
            res = fn(request)
            return res
        except Exception as err:
            raise ConsumerHttpException(repr(err), 500)

    def dispatch_resource_call(
        self,
        tenant=None,
        _type=None,
        operation=None,
        _id=None,
        request=None
    ):
        return self.resources.dispatch(tenant, _type, operation, _id, request)

    #############
    #
    # Listening and Callbacks
    #
    #############

    # register generic listeners on a redis path

    def listen_for_job_change(self):
        _path = self.job_class.reference.redis_path
        LOG.debug(f'Registering Job Change Listener, {_path}')
        self.task.subscribe(self.on_job_change, _path, True)

    def listen_for_resource_change(self):
        # handles work for all resource types
        for _cls in self.job_class._resources:
            redis_path = _cls.reference.redis_path
            _type = _cls.reference.redis_type
            LOG.debug(f'Listening for resource "{_type}"" on {redis_path}')
            self.task.subscribe(self.on_resource_change, redis_path, True)

    # generic handler called on change of any resource. Dispatched to dependent jobs
    def on_resource_change(
        self,
        msg: Union[Task, TaskEvent]
    ) -> None:
        LOG.debug(f'Resource Change: {msg}')
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
