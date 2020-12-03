# Copyright (C) 2020 by eHealth Africa : http://www.eHealthAfrica.org
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

from abc import ABC
from enum import Enum
import json
# import operator
from typing import Any, Dict, get_type_hints, List, Optional, Union  # noqa

# CFS Query API Implementation
'''
This set of classes allow the StructuredQuery Language used by the CFS REST
Interface to be accepted by the API for the purpose of filtering, ordering
and limiting data. These queries build upon a base query which is already
referenced to a particular datatype and filtered for the user based on the RBAC
rules set in logiak. As such, there is no ability to set the initial path with
CFS and the `select` directive is not accepted.
'''

import logging

LOG = logging.getLogger('QRY')
LOG.setLevel(logging.DEBUG)


def format_meta(meta):
    base = {
        k: meta.get(k) for k in [
            'title',
            'description',
            'default',
            'examples'
        ]
    }
    return dict(**{
        'id': meta['$id'],

    }, **base)


class ValueEnum(str, Enum):

    @classmethod
    def validate(cls, v):
        try:
            return cls.lookup[v]
        except KeyError:
            raise ValueError('invalid value')

    @classmethod
    def __get_validators__(cls):
        cls.lookup = {k: v.value for k, v in cls.__members__.items()}
        yield cls.validate


class ValueType(ValueEnum):
    BOOLEAN = 'boolean'
    STRING = 'string'
    NUMBER = 'number'
    NULL = 'null'


class JSchemaType(ABC):
    id_: str
    schema: str
    meta: str
    required: bool
    value: Any

    def __init__(self, obj: Dict):
        self.value = obj

    def toJSON(self):
        return str(self.value)


class JSchemaObject(JSchemaType):
    value: Dict[str, 'SchemaNode']

    def __init__(self, obj: Dict):
        # LOG.info(f'OBJ init {json.dumps(obj, indent=2)}')
        self.schema = obj
        self.meta = {k: v for k, v in obj.items() if k != 'properties'}
        self.value = {}
        for name, sub in obj['properties'].items():
            self.value[name] = SchemaNode(sub)

    def toJSON(self):
        return {
            'meta': format_meta(self.schema),
            'children': {k: v.toJSON() for k, v in self.value.items()}
        }


class JSchemaList(JSchemaType):
    value: JSchemaType


class JSchemaValueField(JSchemaType):
    value: Union[ValueType, List[ValueType]]

    def __init__(self, obj: Dict):
        self.meta = obj
        self.schema = obj
        type_ = obj['type']
        if isinstance(type_, list):
            self.value = [ValueType(i) for i in type_]
        else:
            self.value = ValueType(type_)

    def toJSON(self):
        return {
            'meta': format_meta(self.meta),
            'value': str(self.value.value),
            'input_type': 'field'
        }


class JSchemaChoice(JSchemaType):

    def __init__(self, obj: Dict):
        self.value = {}
        for k, v in obj.items():
            self.value[k] = [SchemaNode(i) for i in v]

    def toJSON(self):
        res = {}
        for k, v in self.value.items():
            res[k] = [i.toJSON() for i in v]
        return res


class JSchemaReference(JSchemaType):

    def toJSON(self):
        return {
            # 'meta': format_meta(self.meta),
            'value': str(self.value.get('$ref')),
            'input_type': 'reference'
        }


class SchemaNode(object):
    node: JSchemaType
    references: Optional[Dict[str, JSchemaType]]

    def __init__(self, schema: Union[str, Dict]):
        # LOG.error(f'init {schema}')
        try:
            obj = schema if not isinstance(schema, str) else json.loads(schema)
        except json.decoder.JSONDecodeError as der:
            LOG.error(schema)
            raise der
        if not isinstance(obj, dict):
            LOG.error(obj)
            raise ValueError('invalid base schema')
        try:
            if '$ref' in obj:
                self.node = JSchemaReference(obj)
            elif 'oneOf' in obj or 'anyOf' in obj:
                self.node = JSchemaChoice(obj)
            elif obj['type'] == 'object':
                self.node = JSchemaObject(obj)
            elif obj['type'] == 'array':
                self.node = JSchemaList(obj)
            else:
                self.node = JSchemaValueField(obj)

        except KeyError as ker:
            LOG.error(json.dumps(obj, indent=2))
            raise ker

    def toJSON(self):
        return self.node.toJSON()
