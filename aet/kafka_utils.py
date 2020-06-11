#!/usr/bin/env python

# Copyright (C) 2019 by eHealth Africa : http://www.eHealthAfrica.org
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

import concurrent
import io
import json
import socket

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from spavro.datafile import DataFileWriter, DataFileReader
from spavro.io import DatumWriter, DatumReader
from spavro.io import validate

from .logger import get_logger

LOG = get_logger('KafkaUtils')


def get_admin_client(kafka_settings):
    return AdminClient(kafka_settings)


def get_producer(kafka_settings):
    return Producer(**kafka_settings)


# see if kafka's port is available
def is_kafka_available(kafka_url, kafka_port):
    kafka_port = int(kafka_port)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((kafka_url, kafka_port))
    except (InterruptedError,
            ConnectionRefusedError,
            socket.gaierror) as rce:
        LOG.debug(
            "Could not connect to Kafka on url: %s:%s" % (kafka_url, kafka_port))
        LOG.debug("Connection problem: %s" % rce)
        return False
    return True


def get_broker_info(kclient, scope='all'):
    try:
        res = {'brokers': [], 'topics': []}
        md = kclient.list_topics(timeout=10)
        for b in iter(md.brokers.values()):
            if b.id == md.controller_id:
                res['brokers'].append("{}  (controller)".format(b))
            else:
                res['brokers'].append("{}".format(b))
        for t in iter(md.topics.values()):
            t_str = []
            if t.error is not None:
                errstr = ": {}".format(t.error)
            else:
                errstr = ""

            t_str.append("{} with {} partition(s){}".format(t, len(t.partitions), errstr))

            for p in iter(t.partitions.values()):
                if p.error is not None:
                    errstr = ": {}".format(p.error)
                else:
                    errstr = ""

                t_str.append(
                    f"partition {p.id} leader: {p.leader}, "
                    f"replicas: {p.replicas}, isrs: {p.isrs}, err: {errstr}"
                )
            res['topics'].append(t_str)
        if scope in res.keys():
            return res[scope]
        return res
    except Exception as err:
        return {'error': f'{err}'}


def create_topic(kadmin, topic_name, partitions=1, replication_factor=3, topic_config=None):
    LOG.debug(f'Trying to create topic {topic_name}')
    if not topic_config:
        topic_config = {
            'retention.ms': -1
        }
    topic = NewTopic(
        topic_name,
        num_partitions=partitions,
        replication_factor=replication_factor,
        config=topic_config
    )
    fs = kadmin.create_topics([topic])
    # future must return before timeout
    for f in concurrent.futures.as_completed(iter(fs.values()), timeout=60):
        e = f.exception()
        if not e:
            LOG.debug(f'Created topic {topic_name}')
            return True
        else:
            LOG.debug(f'Topic {topic_name} could not be created: {e}')
            return False


def delete_topic(kadmin, topic_name):
    fs = kadmin.delete_topics([topic_name], operation_timeout=10)

    # Wait for operation to finish.
    for f in concurrent.futures.as_completed(iter(fs.values()), timeout=60):
        e = f.exception()
        if not e:
            LOG.debug(f'Deleted topic {topic_name}')
            return True
        else:
            LOG.debug(f'Topic {topic_name} could not be Deleted: {e}')
            return False


def kafka_callback(err=None, msg=None, _=None, **kwargs):
    if err:
        LOG.debug('ERROR %s', [err, msg, kwargs])
    with io.BytesIO() as obj:
        obj.write(msg.value())
        reader = DataFileReader(obj, DatumReader())
        for message in reader:
            _id = message.get("id")
            if err:
                LOG.error(f'NO-SAVE: {_id} in | err {err.name()}')


def produce(docs, schema, topic_name, producer, callback=None):
    if not callback:
        callback = kafka_callback
    with io.BytesIO() as bytes_writer:
        writer = DataFileWriter(
            bytes_writer, DatumWriter(), schema, codec='deflate')
        _ids = []
        for row in docs:
            _id = row['id']
            _ids.append(_id)
            msg = row
            if validate(schema, msg):
                writer.append(msg)
            else:
                # Message doesn't have the proper format for the current schema.
                LOG.debug(
                    f"SCHEMA_MISMATCH:NOT SAVED! TOPIC:{topic_name}, ID:{_id}")
        writer.flush()
        raw_bytes = bytes_writer.getvalue()

    producer.poll(0)
    producer.produce(
        topic_name,
        raw_bytes,
        callback=callback,
        headers={
            'avro_size': str(len(_ids)),
            'contains_id': json.dumps(_ids)
        }
    )
