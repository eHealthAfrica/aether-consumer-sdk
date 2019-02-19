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

from datetime import datetime
import json

import redis


class TaskHelper(object):

    def __init__(self, settings):
        self.settings = settings
        self.redis = redis.Redis(
            host=settings.get('REDIS_HOST'),
            port=settings.get('REDIS_PORT'),
            password=settings.get('REDIS_PASSWORD'),
            db=settings.get('REDIS_DB'),
            encoding="utf-8",
            decode_responses=True
        )

    # Generic Redis Task Functions
    def add(self, task, type):
        key = '_{type}:{_id}'.format(
            type=type, _id=task['id']
        )
        task['modified'] = datetime.now().isoformat()
        return self.redis.set(key, json.dumps(task))

    def exists(self, _id, type):
        task_id = '_{type}:{_id}'.format(
            type=type,
            _id=_id
        )
        if self.redis.exists(task_id):
            return True
        return False

    def remove(self, _id, type):
        task_id = '_{type}:{_id}'.format(
            type=type,
            _id=_id
        )
        res = self.redis.delete(task_id)
        if not res:
            return False
        return True

    def get(self, _id, type):
        task_id = '_{type}:{_id}'.format(
            type=type,
            _id=_id
        )
        task = self.redis.get(task_id)
        if not task:
            raise ValueError('No task with id {task_id}'.format(task_id=task_id))
        return json.loads(task)

    def list(self, type=None):
        # jobs as a generator
        if type:
            key_identifier = '_{type}:*'.format(type=type)
        else:
            key_identifier = '*'
        for i in self.redis.scan_iter(key_identifier):
            yield str(i).split(key_identifier[:-1])[1]
