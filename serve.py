#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
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
from aet.consumer import BaseConsumer
from aet.settings import CONSUMER_CONFIG, KAFKA_CONFIG
from tests import IJob


class LocalConsumer(BaseConsumer):

    def __init__(self, consumer_conf, kafka_conf):
        self.job_class = IJob
        super(LocalConsumer, self).__init__(
            consumer_conf,
            kafka_conf,
            self.job_class
        )


if __name__ == '__main__':
    manager = LocalConsumer(
        CONSUMER_CONFIG, KAFKA_CONFIG)
    while True:
        try:
            for x in range(10):
                sleep(1)
            else:
                break
        except KeyboardInterrupt:
            manager.stop()
