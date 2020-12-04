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
    required: bool
    value: Any

    def __init__(self, obj: Dict):
        self.value = obj
        self._set_meta(obj)

    def _set_meta(self, obj: Dict):
        for k in [
            'title',
            'type',
            'description',
            'default',
            'examples',
            'additionalItems',
            'additionalProperties'
        ]:
            if k in obj:
                setattr(self, k, obj.get(k))
        self.id = obj.get("$id")
        if self.id:
            self.name = self.id.split('/')[-1]

    def _get_meta(self):
        return {k: getattr(self, k) for k in [
            'title',
            'name',
            'type',
            'description',
            'default',
            'examples',
            'reference',
            'additionalItems',
            'additionalProperties',
            'id'] if hasattr(self, k)}

    def reference_to(self, parent: 'JSchemaType', bucket: str):
        try:
            self.id = self.id.replace('#/', '')
            self.id = self.id.replace('#', '')
            self.id = self.id.replace('/properties', '')
            self.id = self.id.replace('properties', '')
            pend = [i for i in parent.id.split('/')][-1]
            if pend in self.id:
                spot = self.id.index(pend)
                self.id = parent.id + self.id[(spot + len(pend)):]
            else:
                self.id = parent.id + '/' + self.id if not self.id.startswith('/') \
                    else parent.id + self.id
        except Exception as err:
            LOG.debug([err, type(self)])
        return self.toJSON()

    def toJSON(self):
        return str(self.value)


class JSchemaObject(JSchemaType):
    value: Dict[str, 'SchemaNode']

    def __init__(self, obj: Dict):
        # LOG.info(f'OBJ init {json.dumps(obj, indent=2)}')
        self.schema = obj
        self._set_meta(obj)
        if not hasattr(self, 'additionalProperties'):
            self.additionalProperties = True
        self.value = {'fields': {}, 'definitions': {}}
        for name, sub in obj.get('properties', {}).items():
            self.value['fields'][name] = SchemaNode(sub)
        for name, sub in obj.get('definitions', {}).items():
            self.value['definitions'][name] = SchemaNode(sub)

    def toJSON(self):
        meta = self._get_meta()
        if self.value.get('fields', {}):
            meta['fields'] = [
                v.reference_to(self, 'properties')
                for v in self.value.get('fields', {}).values()]
        if self.value.get('definitions', {}):
            meta['definitions'] = [
                v.reference_to(self, 'definitions')
                for v in self.value.get('definitions', {}).values()]
        return meta


class JSchemaList(JSchemaType):
    value: JSchemaType

    def __init__(self, obj):
        self.schema = obj
        self._set_meta(obj)
        self.value = obj.get('items', {}).get('type')
        if not self.value:
            self.reference = obj.get('items', {}).get('$ref')
            self.value = 'reference'

    def toJSON(self):
        return {
            **self._get_meta(),
            'value': self.value,
        }


class JSchemaValueField(JSchemaType):
    value: Union[ValueType, List[ValueType]]

    def __init__(self, obj: Dict):
        self.schema = obj
        self._set_meta(obj)
        type_ = obj['type']
        if isinstance(type_, list):
            self.value = [ValueType(i) for i in type_]
        else:
            self.value = ValueType(type_)

    def toJSON(self):
        return {k: v for k, v in self.__dict__.items() if k != 'schema'}


class JSchemaChoice(JSchemaType):

    def __init__(self, obj: Dict):
        self.value = {}
        for k, v in obj.items():
            # we don't care about optional requirements, so we expect field references here
            if isinstance(v, list):
                self.value[k] = [SchemaNode(i) for i in v]

    def toJSON(self):
        return [i.toJSON() for k, v in self.value.items() for i in v]


class JSchemaReference(JSchemaType):

    def toJSON(self):
        return {
            # 'meta': format_meta(self.meta),
            'reference': str(self.value.get('$ref')),
            'value': 'reference'
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
            elif 'type' in obj and obj['type'] == 'object':
                self.node = JSchemaObject(obj)
            elif 'type' in obj and obj['type'] == 'array':
                self.node = JSchemaList(obj)
            elif 'oneOf' in obj or 'anyOf' in obj:
                self.node = JSchemaChoice(obj)
            elif 'required' in obj:
                LOG.error(obj)
                self.node = None
            else:
                LOG.debug('unhandled node')
                LOG.debug(obj)
                self.node = JSchemaValueField(obj)

        except KeyError as ker:
            LOG.error(json.dumps(obj, indent=2))
            raise ker

    def reference_to(self, parent: 'JSchemaType', bucket: str):
        return self.node.reference_to(parent, bucket)

    def toJSON(self):
        if self.node is not None:
            return self.node.toJSON()
