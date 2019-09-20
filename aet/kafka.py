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

import ast
from dataclasses import dataclass
import io
import json
from typing import List


import confluent_kafka
from spavro.datafile import DataFileReader
from spavro.io import DatumReader
from spavro.schema import AvroException

from jsonpath_ng import parse

from .logger import get_logger

LOG = get_logger('Kafka')


@dataclass
class Message:
    key: str = None
    value: str = None
    offset: int = None
    topic: str = None
    partition: int = None
    schema: str = None
    headers: List = None


class KafkaConsumer(confluent_kafka.Consumer):

    # Adding these key/ value pairs to those handled by vanilla KafkaConsumer
    ADDITIONAL_CONFIG = {
        "aether_masking_schema_annotation": "aetherMaskingLevel",
        "aether_masking_schema_levels": [0, 1, 2, 3, 4, 5],
        "aether_masking_schema_emit_level": 0,
        "aether_emit_flag_required": True,
        "aether_emit_flag_field_path": "$.approved",
        "aether_emit_flag_values": [True]
    }

    def __init__(self, **kwargs):
        self.config = {}
        # Items not in either default or additional config raise KafkaConfigurationError on super
        for k, v in KafkaConsumer.ADDITIONAL_CONFIG.items():
            if k in kwargs:
                self.config[k] = kwargs[k]
                del kwargs[k]
            else:
                self.config[k] = v
        super(KafkaConsumer, self).__init__(**kwargs)

    def get_approval_filter(self):
        # If {aether_emit_flag_required} is True, each message is checked for a passing value.
        # An approval filter is a flag set in the body of each message and found at path
        # {aether_emit_flag_field_path} that controls whether a message is published or not. If the
        # value found at path {aether_emit_flag_field_path} is not a member of the set configured
        # at {aether_emit_flag_values}, then the message will not be published. If the value is in
        # the set {aether_emit_flag_values}, it will be published. These rules resolve to a simple
        # boolean filter which is returned by this function.
        requires_approval = self.config.get("aether_emit_flag_required")
        if not requires_approval:
            def approval_filter(obj):
                return True
            return approval_filter
        check_condition_path = self.config.get("aether_emit_flag_field_path")
        pass_conditions = self.config.get("aether_emit_flag_values")
        check = None
        if isinstance(pass_conditions, list):
            def check(x):
                return x in pass_conditions
        else:
            def check(x):
                return x == pass_conditions
        expr = parse(check_condition_path)

        def approval_filter(msg):
            values = [match.value for match in expr.find(msg)]
            if not len(values) > 0:
                return False
            return check(values[0])  # We only check the first matching path/ value
        return approval_filter

    def get_mask_from_schema(self, schema):
        # This creates a masking function that will be applied to all messages emitted
        # in poll_and_deserialize. Fields that may need to be masked must have in their
        # schema a signifier of their classification level. The jsonpath of that classifier
        # (mask_query) should be the same for all fields in a schema. Within the the message,
        # any field requiring classification should have a value associated with its field
        # level classification. That classification should match one of the levels passes to
        # the consumer (mask_levels). Fields over the approved classification (emit_level) as
        # ordered in (mask_levels) will be removed from the message before being emitted.

        mask_query = self.config.get("aether_masking_schema_annotation")  # classifier jsonpath
        mask_levels = self.config.get("aether_masking_schema_levels")     # classifier levels
        emit_level = self.config.get("aether_masking_schema_emit_level")  # chosen level
        try:
            emit_index = mask_levels.index(emit_level)
        except ValueError:
            emit_index = -1  # emit level is off the scale, so we don't emit any classified data
        query_string = "$.fields.[*].%s.`parent`" % mask_query  # parent node of matching field
        expr = parse(query_string)
        restricted_fields = [(match.value) for match in expr.find(schema)]
        restriction_map = [[obj.get("name"), obj.get(mask_query)] for obj in restricted_fields]
        failing_values = [i[1] for i in restriction_map if mask_levels.index(i[1]) > emit_index]

        def mask(msg):
            for name, field_level in restriction_map:
                if msg.get(name, None):  # message has a field with classification
                    if field_level in failing_values:  # classification is above threshold
                        msg.pop(name, None)
            return msg
        return mask

    def mask_message(self, msg, mask=None):
        # this applies a mask created from get_mask_from_schema()
        if not mask:
            return msg
        else:
            return mask(msg)

    def poll_and_deserialize(self, num_messages=1, timeout=1):
        # None of the methods in the Python Kafka library deserialize messages, which is a
        # required step in order to filter fields which may be masked, or to only publish
        # messages which meet a certain condition. For this reason, we extend the poll() method
        # from the Kafka library to handle deserialzation in a fast and reliable way. We also
        # implement masking and field filtering in this method, based on the consumer configuration
        # passed in __init__ and the schema of each message.

        last_schema = None
        mask = None
        approval_filter = None
        result = []
        incoming = self.consume(num_messages=num_messages, timeout=timeout)
        for m in incoming:
            key = m.key()
            partition = m.partition()
            offset = m.offset()
            headers = m.headers()
            topic = m.topic()
            obj = io.BytesIO()
            obj.write(m.value())
            try:
                reader = DataFileReader(obj, DatumReader())
                (
                    package_result,
                    last_schema,
                    mask,
                    approval_filter
                ) = \
                    self._reader_to_messages(
                        reader,
                        last_schema,
                        mask,
                        approval_filter
                )
                obj.close()  # don't forget to close your open IO object.
                if package_result.get("schema") or len(package_result["messages"]) > 0:
                    schema = package_result.get("schema")
                    for message_body in package_result['messages']:
                        result.append(Message(
                            key,
                            message_body,
                            offset,
                            topic,
                            partition,
                            schema,
                            headers
                        ))
            except AvroException:
                reader = None
                (
                    package_result,
                    last_schema,
                    mask,
                    approval_filter
                ) = (
                    self._unpack_bytes_message(obj),
                    None,
                    None,
                    None
                )
                obj.close()  # don't forget to close your open IO object.
                if len(package_result["messages"]) > 0:
                    for message_body in package_result['messages']:
                        result.append(Message(
                            key,
                            message_body,
                            offset,
                            topic,
                            partition,
                            None,
                            headers
                        ))
        return result

    def _reader_to_messages(self, reader, last_schema, mask, approval_filter):
        package_result = {
            "schema": None,
            "messages": []
        }
        # We can get the schema directly from the reader.
        # we get a mess of unicode that can't be json parsed so we need ast
        raw_schema = ast.literal_eval(str(reader.meta))
        schema = json.loads(raw_schema.get("avro.schema"))
        if schema != last_schema:
            last_schema = schema
            package_result["schema"] = schema
            # prepare mask and filter
            approval_filter = self.get_approval_filter()
            mask = self.get_mask_from_schema(schema)
        else:
            package_result["schema"] = last_schema
        for x, msg in enumerate(reader):
            # is message is ready for consumption, process it
            if approval_filter(msg):
                # apply masking
                processed_message = self.mask_message(msg, mask)
                package_result["messages"].append(processed_message)

        return package_result, last_schema, mask, approval_filter

    def _unpack_bytes_message(self, reader):
        package_result = {
            "schema": None,
            "messages": [self._read_json(self._decode_text(reader))]
        }
        return package_result

    def _decode_text(self, reader):
        try:
            return reader.getvalue().decode('utf-8', 'strict')
        except UnicodeDecodeError:
            pass
        return reader.getvalue().decode('ascii', 'strict')  # raises UnicodeDecodeError

    def _read_json(self, raw_text):
        try:
            return json.loads(raw_text)
        except json.decoder.JSONDecodeError:
            return raw_text

    def seek_to_beginning(self):
        # We override this method to allow for seeking before any messages have been consumed
        # as poll consumes a message. Since we're going to change offset, we don't care.
        self.poll(timeout=1)
        partitions = self.assignment()
        for p in partitions:
            p.offset = confluent_kafka.OFFSET_BEGINNING
            super(KafkaConsumer, self).assign(p)
