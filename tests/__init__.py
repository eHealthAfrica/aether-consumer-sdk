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
import mock
import os
import pytest
from time import sleep


from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from spavro.datafile import DataFileWriter
from spavro.io import DatumWriter
from spavro.schema import parse as ParseSchema

from aet import settings
from aet.api import APIServer
from aet.consumer import BaseConsumer
from aet.kafka import KafkaConsumer
from aet.task import TaskHelper

from .assets.schemas import test_schemas
from aet.logger import LOG


kafka_server = "kafka-test:29092"
kafka_connection_retry = 10
kafka_connection_retry_wait = 6
# increasing topic_size may cause poll to be unable to get all the messages in one call.
# needs to be even an if > 100 a multiple of 100.
topic_size = 500


class MockCallable(object):
    def __init__(self):
        self.value = None

    def set_value(self, msg):
        LOG.debug(f'MockCallable got msg: {msg}')
        self.value = msg


class MockTaskHelper(object):

    def __init__(self):
        pass

    def add(self, task, type):
        return True

    def exists(self, _id, type):
        return True

    def remove(self, _id, type):
        return True

    def get(self, _id, type):
        return '{}'

    def list(self, type=None):
        return []


class MockConsumer(BaseConsumer):

    PERMISSIVE_SCHEMA = {  # should match anything
        'type': 'object',
        'additionalProperties': True,
        'properties': {
        }
    }

    STRICT_SCHEMA = {  # should match nothing but empty brackets -> {}
        'type': 'object',
        'additionalProperties': False,
        'properties': {
        }
    }

    def __init__(self, CON_CONF, KAFKA_CONF):
        self.consumer_settings = CON_CONF
        self.kafka_settings = KAFKA_CONF
        self.children = []
        self.task = MockTaskHelper()
        self.schema = MockConsumer.STRICT_SCHEMA


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
@pytest.fixture()
def fake_settings():
    _settings = settings.Settings(
        file_path='/code/tests/assets/test_config.json',
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
        "bootstrap_servers": kafka_server,
        "heartbeat_interval_ms": 2500,
        "session_timeout_ms": 18000,
        "request_timeout_ms": 20000,
        "auto_offset_reset": 'latest',
        "consumer_timeout_ms": 17000
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
    with mock.patch('aet.kafka.VanillaConsumer.__init__') as MKafka:
        MKafka.return_value = None  # we need to ignore the call to super in __init__
        consumer = KafkaConsumer()
    consumer._set_config = set_config.__get__(consumer)
    consumer._add_config = add_config.__get__(consumer)
    # somehow the ADDITIONAL_CONFIG changes if you pass it directly.
    # Leave this deepcopy
    consumer._set_config(deepcopy(KafkaConsumer.ADDITIONAL_CONFIG))
    return consumer


# Consumer Assets
@pytest.mark.unit
@pytest.fixture(scope="module")
def mocked_consumer():
    return MockConsumer(settings.CONSUMER_CONFIG, settings.KAFKA_CONFIG)


@pytest.mark.integration
@pytest.fixture(scope="module")
def consumer():
    # mock API from unit tests not shutdown until end avoid it's port and name
    os.environ['CONSUMER_NAME'] = 'BaseConsumer'
    os.environ['EXPOSE_PORT'] = '9014'
    _consumer = BaseConsumer(settings.CONSUMER_CONFIG, settings.KAFKA_CONFIG)
    _consumer.schema = {}  # blank schema
    yield _consumer
    # teardown
    _consumer.stop()
    sleep(.5)


# API Assets
@pytest.mark.unit
@pytest.fixture(scope="module")
def mocked_api(mocked_consumer):
    api = APIServer(
        mocked_consumer,
        mocked_consumer.task,
        settings.CONSUMER_CONFIG
    )
    api.serve()
    yield api
    # teardown
    api.stop()
    sleep(.5)


# TaskHelper Assets
@pytest.mark.integration
@pytest.fixture(scope="function")
def task_helper():
    helper = TaskHelper(settings.CONSUMER_CONFIG)
    yield helper
    helper.stop()
