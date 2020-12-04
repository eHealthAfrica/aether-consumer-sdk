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
import io
import json
import os
import pytest
from time import sleep
from typing import Any, ClassVar, Dict, List, Iterable, Optional  # noqa
from unittest import mock
from uuid import uuid4

from aether.python.redis.task import Task, TaskHelper
from redis import Redis
import fakeredis

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from spavro.datafile import DataFileWriter
from spavro.io import DatumWriter
from spavro.schema import parse as ParseSchema

from aet import settings
from aet.api import APIServer
from aet.consumer import BaseConsumer
from aet.job import BaseJob, JobManager, JobStatus
from aet.jsonpath import CachedParser  # noqa
from aet.kafka import KafkaConsumer
from aet.logger import get_logger
from aet.resource import (  # noqa
    BaseResource,
    BASE_PUBLIC_ACTIONS,
    lock,
    MethodDesc
)


from .assets.schemas import test_schemas

LOG = get_logger('Test')

here = os.path.dirname(os.path.realpath(__file__))

kafka_server = "kafka-test:29092"
kafka_connection_retry = 10
kafka_connection_retry_wait = 6
# increasing topic_size may cause poll to be unable to get all the messages in one call.
# needs to be even an if > 100 a multiple of 100.
topic_size = 500


TestResourceDef1 = {'id': '1', 'username': 'user', 'password': 'pw'}


class BadResource(BaseResource):
    # Missing required attributes
    pass


class BadJob(BaseJob):
    # Missing required attributes
    pass


class TestResource(BaseResource):

    name = 'resource'
    jobs_path = '$.no.real.job'
    _masked_fields = ['password']

    public_actions = BASE_PUBLIC_ACTIONS + [
        'upper',
        'null',
        'validate_pretty'
    ]

    schema = '''
    {
      "definitions": {},
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "http://example.com/root.json",
      "type": "object",
      "title": "The Root Schema",
      "required": [
        "username",
        "password"
      ],
      "properties": {
        "id": {
          "$id": "#/properties/id",
          "type": "string",
          "title": "The ID Schema",
          "default": "",
          "examples": [
            "someid"
          ],
          "pattern": "^(.*)$"
        },
        "username": {
          "$id": "#/properties/username",
          "type": "string",
          "title": "The Username Schema",
          "default": "",
          "examples": [
            "shawn"
          ],
          "pattern": "^(.*)$"
        },
        "password": {
          "$id": "#/properties/password",
          "type": "string",
          "title": "The Password Schema",
          "default": "",
          "examples": [
            "password"
          ],
          "pattern": "^(.*)$"
        }
      }
    }
    '''

    def upper(self, key: str):
        '''
            Returns the uppercase version of the requested key
            OR None if the key is not found
        '''
        try:
            return self.definition.get(key).upper()
        except Exception:
            return None

    @lock
    def null(self, key: str):
        '''
            Returns None, always
        '''
        LOG.debug('Null.')
        return None

    @lock
    def sleepy_lock(self, dur):
        sleep(dur)
        LOG.debug('Wake up.')


class TestJob(BaseJob):
    name = 'TestJob'
    schema = '''
    {
      "type": "object",
      "additionalProperties": false,
      "properties": {}
    }
    '''
    _resources = [TestResource]
    public_actions = BaseJob.public_actions


class IResource(BaseResource):

    jobs_path = '$.resources'
    name = 'resource'

    public_actions = [
        'say_wait'
    ]

    schema = '''
    {
      "definitions": {},
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "http://example.com/root.json",
      "type": "object",
      "title": "The Root Schema",
      "required": [
        "id",
        "say",
        "wait"
      ],
      "properties": {
        "id": {
          "$id": "#/properties/id",
          "type": "string",
          "title": "The Id Schema",
          "default": "",
          "examples": [
            "an-id"
          ],
          "pattern": "^(.*)$"
        },
        "say": {
          "$id": "#/properties/say",
          "type": "string",
          "title": "The Say Schema",
          "default": "",
          "examples": [
            "something"
          ],
          "pattern": "^(.*)$"
        },
        "wait": {
          "$id": "#/properties/wait",
          "type": "integer",
          "title": "The Wait Schema",
          "default": 0,
          "examples": [
            1
          ]
        }
      }
    }
    '''

    @lock
    def say_wait(self):
        sleep(int(self.definition.get('wait')))
        return self.definition.get('say')

    def say(self):
        return self.definition.get('say')


class IJob(BaseJob):

    name = 'job'
    _resources = [IResource]
    schema = '''
    {
      "definitions": {},
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "http://example.com/root.json",
      "type": "object",
      "title": "The Root Schema",
      "required": [
        "id",
        "resources",
        "poll_interval"
      ],
      "properties": {
        "id": {
          "$id": "#/properties/id",
          "type": "string",
          "title": "The Id Schema",
          "default": "",
          "examples": [
            "an-id"
          ],
          "pattern": "^(.*)$"
        },
        "resources": {
          "$id": "#/properties/resources",
          "type": "array",
          "title": "The Resources Schema",
          "items": {
            "$id": "#/properties/resources/items",
            "type": "string",
            "title": "The Items Schema",
            "default": "",
            "examples": [
              "a",
              "b"
            ],
            "pattern": "^(.*)$"
          }
        },
        "poll_interval": {
          "$id": "#/properties/poll_interval",
          "type": "integer",
          "title": "The Poll_interval Schema",
          "default": 0,
          "examples": [
            1
          ]
        }
      }
    }
    '''

    def _get_messages(self, config):
        LOG.debug(f'{self._id} gettings messages')
        messages = []
        resources = self.get_resources('resource', config)
        if not resources:
            raise RuntimeError('No resources!')
        for r in resources:
            val = r.say_wait()  # long running
            if self.status is JobStatus.STOPPED:
                return []
            messages.append(val)
        LOG.debug(f'{self._id} returning messages {messages}')
        return messages

    def _handle_messages(self, config, messages):
        for m in messages:
            LOG.debug(m)

    def share_resource(self, _id, prop):
        r = self.get_resource('resource', _id)
        return r.definition.get(prop)


class StuckJob(IJob):
    def _get_messages(self, config):
        sleep(60)
        return []


class MockCallable(object):
    value: Optional[Task] = None

    def set_value(self, msg: Task):
        LOG.debug(f'MockCallable got msg: {msg}')
        self.value = msg


def get_redis():
    return Redis(host='redis-test')


def get_fakeredis():
    return fakeredis.FakeStrictRedis()


class MockConsumer(BaseConsumer):
    def __init__(self, CON_CONF, KAFKA_CONF):
        self.job_class = TestJob
        super(MockConsumer, self).__init__(
            CON_CONF,
            KAFKA_CONF,
            self.job_class,
            redis_instance=get_redis()
        )


class MockStuckConsumer(BaseConsumer):
    def __init__(self, CON_CONF, KAFKA_CONF):
        _settings = CON_CONF.copy()
        CON = settings.Settings(_settings)
        CON.override('MAX_JOB_IDLE_SEC', 1)
        CON.override('DUMP_STACK_ON_TIMEOUT', True)
        CON.override('EXPOSE_PORT', 7099)
        LOG.debug(CON.copy())
        self.job_class = StuckJob
        super(MockStuckConsumer, self).__init__(
            CON,
            KAFKA_CONF,
            self.job_class,
            redis_instance=get_redis()
        )


def send_plain_messages(producer, topic, schema, messages, encoding, is_json=True):
    for msg in messages:
        if is_json:
            val = json.dumps(msg).encode(encoding)
        else:
            val = msg['id'].encode(encoding)
        future = producer.send(
            topic,
            key=str(msg.get("id")),
            value=val
        )
        # block until it actually sends.
        record_metadata = future.get(timeout=100)
        if not record_metadata:
            pass
    producer.flush()


def send_avro_messages(producer, topic, schema, messages):
    bytes_writer = io.BytesIO()
    writer = DataFileWriter(bytes_writer, DatumWriter(), schema, codec='deflate')
    for msg in messages:
        writer.append(msg)
    writer.flush()
    raw_bytes = bytes_writer.getvalue()
    writer.close()
    future = producer.send(topic, key=str(msg.get("id")), value=raw_bytes)
    # block until it actually sends.
    record_metadata = future.get(timeout=100)
    if not record_metadata:
        pass  # we may want to check the metadata in the future
    producer.flush()


@pytest.mark.unit
@pytest.fixture(scope="session")
def TaskHelperSessionScope():
    task = TaskHelper(
        settings.CONSUMER_CONFIG,
        get_fakeredis()
    )
    yield task
    LOG.warning('Destroying FakeRedis')
    task.stop()


@pytest.mark.unit
@pytest.fixture(scope="session")
def IJobManager(TaskHelperSessionScope):
    task = TaskHelperSessionScope
    man = JobManager(task, IJob)
    yield man
    LOG.warning('destroying IMAN')
    man.stop()


@pytest.mark.unit
@pytest.fixture(scope="function")
def IJobManagerFNScope(TaskHelperSessionScope):
    task = TaskHelperSessionScope
    man = JobManager(task, IJob)
    yield man
    LOG.warning('destroying IMAN fn scope')
    man.stop()


@pytest.mark.unit
@pytest.fixture()
def fake_settings():
    _settings = settings.Settings(
        file_path=os.path.join(here, 'assets/test_config.json'),
        alias={'D': 'B'},
        exclude=['B']
    )
    return _settings


@pytest.mark.integration
@pytest.fixture(scope="session")
def producer():
    producer = None
    for x in range(kafka_connection_retry):
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_server,
                                     acks=1, key_serializer=str.encode)
            break
        except NoBrokersAvailable:
            sleep(kafka_connection_retry_wait)
    if not producer:
        raise NoBrokersAvailable("Could not attach to kafka after %s seconds. Check configuration" %
                                 (kafka_connection_retry * kafka_connection_retry_wait))
    yield producer
    producer.close()


@pytest.mark.integration
@pytest.fixture(scope="session")
def topic_writer(producer):
    def _fn(topic=None, source=None, encoding='avro', is_json=True, include_schema=True):
        assets = test_schemas.get(source)
        if include_schema:
            schema = assets.get("schema")
            schema = ParseSchema(json.dumps(schema, indent=2))
        else:
            schema = None
        mocker = assets.get("mocker")
        messages = []
        # the parcel gets large if you stick too many messages in it.
        # 100 serialzed together effectively minimizes the impact of passing the schema.
        if topic_size > 100:
            for x in range(0, topic_size, 100):
                batch = mocker(count=100)
                messages.extend(batch)
                if encoding == 'avro':
                    send_avro_messages(producer, topic, schema, batch)
                else:
                    send_plain_messages(
                        producer, topic, schema, messages, encoding=encoding, is_json=is_json)
        else:
            messages = mocker(count=topic_size)
            if encoding == 'avro':
                send_avro_messages(producer, topic, schema, messages)
            else:
                send_plain_messages(
                    producer, topic, schema, messages, encoding=encoding, is_json=is_json)
        return messages
    return _fn


@pytest.mark.integration
@pytest.fixture(scope="function")
def default_consumer_args():
    return deepcopy({
        "aether_masking_schema_annotation": "aetherMaskingLevel",
        "aether_emit_flag_field_path": "$.publish",
        "aether_emit_flag_values": [True, False],
        "aether_masking_schema_levels": [0, 1, 2, 3, 4, 5],
        "aether_masking_schema_emit_level": 0,
        "group.id": str(uuid4()),
        "bootstrap.servers": kafka_server,
        "auto.offset.reset": 'earliest'
    })


@pytest.mark.integration
@pytest.fixture(scope="session")
def messages_test_json_utf8(topic_writer):
    topic = "TestJSONMessagesUTF"
    src = "TestBooleanPass"
    messages = topic_writer(topic=topic, source=src, encoding='utf-8')
    return messages


@pytest.mark.integration
@pytest.fixture(scope="session")
def messages_test_json_ascii(topic_writer):
    topic = "TestJSONMessagesASCII"
    src = "TestBooleanPass"
    messages = topic_writer(topic=topic, source=src, encoding='ascii')
    return messages


@pytest.mark.integration
@pytest.fixture(scope="session")
def messages_test_text_utf8(topic_writer):
    topic = "TestPlainMessagesUTF"
    src = "TestBooleanPass"
    messages = topic_writer(topic=topic, source=src, encoding='utf-8', is_json=False)
    return messages


@pytest.mark.integration
@pytest.fixture(scope="session")
def messages_test_text_ascii(topic_writer):
    topic = "TestPlainMessagesASCII"
    src = "TestBooleanPass"
    messages = topic_writer(topic=topic, source=src, encoding='ascii', is_json=False)
    return messages


@pytest.mark.integration
@pytest.fixture(scope="session")
def messages_test_boolean_pass(topic_writer):
    topic = "TestBooleanPass"
    messages = topic_writer(topic=topic, source=topic)
    return messages


@pytest.mark.integration
@pytest.fixture(scope="session")
def messages_test_enum_pass(topic_writer):
    topic = "TestEnumPass"
    messages = topic_writer(topic=topic, source=topic)
    return messages


@pytest.mark.integration
@pytest.fixture(scope="session")
def messages_test_secret_pass(topic_writer):
    topic = "TestTopSecret"
    messages = topic_writer(topic=topic, source=topic)
    return messages


@pytest.mark.unit
@pytest.fixture(scope="session")
def sample_schema():
    assets = test_schemas.get("TestBooleanPass")
    return assets.get("schema")


@pytest.mark.unit
@pytest.fixture(scope="function")
def sample_message():
    assets = test_schemas.get("TestBooleanPass")
    mocker = assets.get("mocker")
    yield mocker()[0]


@pytest.mark.unit
@pytest.fixture(scope="session")
def sample_schema_top_secret():
    assets = test_schemas.get("TestTopSecret")
    return assets.get("schema")


@pytest.mark.unit
@pytest.fixture(scope="function")
def sample_message_top_secret():
    assets = test_schemas.get("TestTopSecret")
    mocker = assets.get("mocker")
    yield mocker()[0]


@pytest.mark.unit
@pytest.fixture(scope="function")
def offline_consumer():
    consumer = None

    def set_config(self, new_configs):
        self.config = new_configs

    def add_config(self, pairs):
        for k, v in pairs.items():
            self.config[k] = v
    # Mock up a usable KafkaConsumer that doesn't use Kafka...
    with mock.patch('aet.kafka.KafkaConsumer.__init__') as MKafka:
        MKafka.return_value = None  # we need to ignore the call to super in __init__
        consumer = KafkaConsumer()
    consumer._set_config = set_config.__get__(consumer)
    consumer._add_config = add_config.__get__(consumer)
    # somehow the ADDITIONAL_CONFIG changes if you pass it directly.
    # Leave this deepcopy
    _configs = deepcopy(KafkaConsumer.ADDITIONAL_CONFIG)
    _configs['aether_emit_flag_required'] = True  # we don't need to test the all_pass state
    consumer._set_config(_configs)
    return consumer


# Consumer Assets
@pytest.mark.unit
@pytest.fixture(scope="module")
def mocked_consumer():
    consumer = MockConsumer(settings.CONSUMER_CONFIG, settings.KAFKA_CONFIG)
    yield consumer
    LOG.debug('Fixture mocked_consumer complete, stopping.')
    consumer.stop()
    sleep(.5)


@pytest.mark.unit
@pytest.fixture(scope="module")
def mocked_stuck_consumer():
    consumer = MockStuckConsumer(settings.CONSUMER_CONFIG, settings.KAFKA_CONFIG)
    LOG.debug('Starting mocked_stuck_consumer')
    yield consumer
    LOG.debug('Fixture mocked_stuck_consumer complete, stopping.')
    consumer.stop()
    sleep(.5)


# API Assets
@pytest.mark.unit
@pytest.fixture(scope="module")
def mocked_api(mocked_consumer) -> Iterable[APIServer]:
    yield mocked_consumer.api
    # teardown
    LOG.debug('Fixture api complete, stopping.')
    mocked_consumer.stop()
    # api.stop()
    sleep(.5)


@pytest.mark.unit
@pytest.fixture(scope="module")
def mocked_stuck_api(mocked_stuck_consumer) -> Iterable[APIServer]:
    yield mocked_stuck_consumer.api
    # teardown
    LOG.debug('Fixture api complete, stopping.')
    mocked_stuck_consumer.stop()
    # api.stop()
    sleep(.5)


NODE_RESOURCE_SCHEMA_EASY = '''
{
    "$schema": "http://json-schema.org/draft-07/schema",
    "$id": "stream-consumer/restcall.json",
    "type": "object",
    "title": "RestCall",
    "description": "A Stream Consumer RestCall",
    "default": {},
    "additionalProperties": true,
    "required": [
        "id",
        "name"
    ],
    "properties": {
        "id": {
            "$id": "#/properties/id",
            "type": "string",
            "title": "ID",
            "description": "The ID used to reference the instance",
            "default": "",
            "examples": [
                "default"
            ]
        },
        "name": {
            "$id": "#/properties/name",
            "type": "string",
            "title": "Name",
            "description": "A description for the resource. Can be multiple works / include spaces",
            "default": "",
            "examples": [
                "Some Long Name"
            ]
        }
    }
}
'''

NODE_RESOURCE_SCHEMA_MED = '''
{
    "$schema": "http://json-schema.org/draft-07/schema",
    "$id": "stream-consumer/jscall.json",
    "type": "object",
    "title": "Javascript Call",
    "description": "A Stream Consumer Javascript Call",
    "default": {},
    "additionalProperties": true,
    "required": [
        "id",
        "name",
        "arguments",
        "entrypoint"
    ],
    "properties": {
        "id": {
            "$id": "#/properties/id",
            "type": "string",
            "title": "ID",
            "description": "The ID used to reference the instance",
            "default": "",
            "examples": [
                "default"
            ]
        },
        "name": {
            "$id": "#/properties/name",
            "type": "string",
            "title": "Name",
            "description": "A description for the resource. Can be multiple works / include spaces",
            "default": "",
            "examples": [
                "Some Long Name"
            ]
        },
        "entrypoint": {
            "$id": "#/properties/entrypoint",
            "type": "string",
            "title": "The Entrypoint Schema",
            "description": "An explanation about the purpose of this instance.",
            "default": "",
            "examples": [
                "f"
            ]
        },
        "script": {
            "$id": "#/properties/script",
            "type": "string",
            "title": "The Script Schema",
            "description": "An explanation about the purpose of this instance.",
            "default": "",
            "examples": [
                ""
            ]
        },
        "arguments": {
                "oneOf": [
                    {"$ref": "#/definitions/argumentList"},
                    {"$ref": "#/definitions/argumentDict"}
                ]
            }
        },
    "definitions": {
        "argumentList": {
            "$id": "#/definitions/argumentList",
            "type": "array",
            "title": "The Arguments Schema",
            "description": "An explanation about the purpose of this instance.",
            "default": [],
            "examples": [
                [
                    "a",
                    "b"
                ]
            ],
            "additionalItems": true,
            "items": {
                "$id": "#/properties/arguments/items",
                "type": "string",
                "title": "The Items Schema",
                "description": "An explanation about the purpose of this instance.",
                "default": "",
                "examples": [
                    "a",
                    "b"
                ]
            }
        },
        "argumentDict": {
            "$id": "#/definitions/argumentDict",
            "type": "object",
            "title": "The Arguments Schema",
            "description": "An explanation about the purpose of this instance."
        }
    }
}
'''

NODE_RESOURCE_SCHEMA_HARD = '''
{
  "$schema": "http://json-schema.org/draft-07/schema",
  "$id": "stream-consumer/pipeline.json",
  "type": "object",
  "title": "The Root Schema",
  "description": "The root schema comprises the entire JSON document.",
  "default": {},
  "additionalProperties": false,
  "anyOf": [
    {
      "required": [
        "id",
        "zeebe_instance",
        "zeebe_subscription",
        "stages"
      ]
    },
    {
      "required": [
        "id",
        "kafka_subscription",
        "stages"
      ]
    }
  ],
  "definitions": {
    "error_handler": {
      "$id": "#definitions/error_handler",
      "type": "object",
      "title": "The Error Handler Schema",
      "required": [
        "error_topic"
      ],
      "additionalProperties": false,
      "properties": {
        "error_topic": {
          "$id": "#/properties/error_handler/properties/error_topic",
          "type": "string",
          "title": "ErrorEventTopic",
          "examples": [
            "The Topic to write error events"
          ],
          "pattern": "[a-z0-9.-]"
        },
        "log_errors": {
          "$id": "#/properties/error_handler/properties/log_errors",
          "type": "boolean",
          "title": "LogErrors",
          "default": true
        },
        "log_success": {
          "$id": "#/properties/error_handler/properties/log_success",
          "type": "boolean",
          "title": "LogSuccess",
          "default": false
        }
      }
    },
    "kafka_subscription": {
      "$id": "#definitions/kafka_subscription",
      "type": "object",
      "title": "The Root Schema",
      "required": [
        "topic_pattern",
        "topic_options"
      ],
      "properties": {
        "topic_pattern": {
          "$id": "#/properties/topic_pattern",
          "type": "string",
          "title": "The Topic_pattern Schema",
          "default": "",
          "examples": [
            "source topic for data i.e. gather*"
          ],
          "pattern": "^(.*)$"
        },
        "topic_options": {
          "$id": "#/properties/topic_options",
          "type": "object",
          "title": "The Topic_options Schema",
          "anyOf": [
            {
              "required": [
                "masking_annotation"
              ]
            },
            {
              "required": [
                "filter_required"
              ]
            }
          ],
          "dependencies": {
            "filter_required": [
              "filter_field_path",
              "filter_pass_values"
            ],
            "masking_annotation": [
              "masking_levels",
              "masking_emit_level"
            ]
          },
          "properties": {
            "masking_annotation": {
              "$id": "#/properties/topic_options/properties/masking_annotation",
              "type": "string",
              "title": "The Masking_annotation Schema",
              "default": "",
              "examples": [
                "@aether_masking"
              ],
              "pattern": "^(.*)$"
            },
            "masking_levels": {
              "$id": "#/properties/topic_options/properties/masking_levels",
              "type": "array",
              "title": "The Masking_levels Schema",
              "items": {
                "$id": "#/properties/topic_options/properties/masking_levels/items",
                "title": "The Items Schema",
                "examples": [
                  "private",
                  "public"
                ],
                "pattern": "^(.*)$"
              }
            },
            "masking_emit_level": {
              "$id": "#/properties/topic_options/properties/masking_emit_level",
              "type": "string",
              "title": "The Masking_emit_level Schema",
              "default": "",
              "examples": [
                "public"
              ],
              "pattern": "^(.*)$"
            },
            "filter_required": {
              "$id": "#/properties/topic_options/properties/filter_required",
              "type": "boolean",
              "title": "The Filter_required Schema",
              "default": false,
              "examples": [
                false
              ]
            },
            "filter_field_path": {
              "$id": "#/properties/topic_options/properties/filter_field_path",
              "type": "string",
              "title": "The Filter_field_path Schema",
              "default": "",
              "examples": [
                "some.json.path"
              ],
              "pattern": "^(.*)$"
            },
            "filter_pass_values": {
              "$id": "#/properties/topic_options/properties/filter_pass_values",
              "type": "array",
              "title": "The Filter_pass_values Schema",
              "items": {
                "$id": "#/properties/topic_options/properties/filter_pass_values/items",
                "title": "The Items Schema",
                "examples": [
                  false
                ]
              }
            }
          }
        }
      }
    },
    "stage": {
      "$id": "#/definitions/stage",
      "type": "object",
      "title": "The Stage Schema",
      "description": "An explanation about the purpose of this instance.",
      "default": {},
      "examples": [
        {
          "name": "one",
          "transition": {
            "fail_condition": "",
            "pass_condition": "",
            "output_map": {},
            "input_map": {}
          },
          "id": "default",
          "type": "restcall"
        }
      ],
      "additionalProperties": true,
      "required": [
        "name",
        "type",
        "id",
        "transition"
      ],
      "properties": {
        "name": {
          "$id": "#/properties/stage/properties/name",
          "type": "string",
          "title": "The Name Schema",
          "description": "An explanation about the purpose of this instance.",
          "default": "",
          "examples": [
            "one"
          ]
        },
        "type": {
          "$id": "#/properties/stage/properties/type",
          "type": "string",
          "title": "The Type Schema",
          "description": "An explanation about the purpose of this instance.",
          "default": "",
          "examples": [
            "restcall"
          ]
        },
        "id": {
          "$id": "#/properties/stage/properties/id",
          "type": "string",
          "title": "The Id Schema",
          "description": "An explanation about the purpose of this instance.",
          "default": "",
          "examples": [
            "default"
          ]
        },
        "transition": {
          "$id": "#/properties/stage/properties/transition",
          "type": "object",
          "title": "The Transition Schema",
          "description": "An explanation about the purpose of this instance.",
          "default": {},
          "examples": [
            {
              "pass_condition": "",
              "output_map": {},
              "input_map": {},
              "fail_condition": ""
            }
          ],
          "additionalProperties": false,
          "required": [],
          "properties": {
            "pass_condition": {
              "$id": "#/properties/stage/properties/transition/properties/pass_condition",
              "type": "string",
              "title": "The Pass_condition Schema",
              "description": "An explanation about the purpose of this instance.",
              "default": "",
              "examples": [
                ""
              ]
            },
            "fail_condition": {
              "$id": "#/properties/stage/properties/transition/properties/fail_condition",
              "type": "string",
              "title": "The Fail_condition Schema",
              "description": "An explanation about the purpose of this instance.",
              "default": "",
              "examples": [
                ""
              ]
            },
            "input_map": {
              "$id": "#/properties/stage/properties/transition/properties/input_map",
              "type": "object",
              "title": "The Input_map Schema",
              "description": "An explanation about the purpose of this instance.",
              "default": {},
              "examples": [
                {}
              ],
              "additionalProperties": true
            },
            "output_map": {
              "$id": "#/properties/stage/properties/transition/properties/output_map",
              "type": "object",
              "title": "The Output_map Schema",
              "description": "An explanation about the purpose of this instance.",
              "default": {},
              "examples": [
                {}
              ],
              "additionalProperties": true
            }
          }
        }
      }
    }},
  "properties": {
    "id": {
      "$id": "#/properties/id",
      "type": "string",
      "title": "The Id Schema",
      "description": "An explanation about the purpose of this instance.",
      "default": "",
      "examples": [
        "default"
      ]
    },
    "name": {
      "$id": "#/properties/name",
      "type": "string",
      "title": "The Name Schema",
      "description": "An explanation about the purpose of this instance.",
      "default": "",
      "examples": [
        "Some Name"
      ]
    },
    "zeebe_instance": {
      "$id": "#/properties/zeebe_instance",
      "type": "string",
      "title": "The Zeebe_instance Schema",
      "description": "An explanation about the purpose of this instance.",
      "default": "",
      "examples": [
        "default"
      ]
    },
    "zeebe_subscription": {
      "$id": "#/properties/zeebe_subscription",
      "type": "string",
      "title": "The Zeebe_subscription Schema",
      "description": "An explanation about the purpose of this instance.",
      "default": "",
      "examples": [
        "rest-worker"
      ]
    },
    "kafka_subscription": {
      "$ref": "#/definitions/kafka_subscription"
    },
    "error_handling": {
      "$ref": "#/definitions/error_handler"
    },
    "const": {
      "$id": "#/properties/const",
      "type": "object",
      "title": "The Const Schema",
      "description": "Constants made available to the pipeline at runtime"
    },
    "stages": {
      "$id": "#/properties/stages",
      "type": "array",
      "title": "The Stages Schema",
      "description": "An explanation about the purpose of this instance.",
      "default": [],
      "examples": [
        []
      ],
      "additionalItems": true,
      "items": {
        "$ref": "#/definitions/stage"
      }
    }
  }
}
'''
