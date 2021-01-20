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

schema_boolean_pass = {
    'fields': [
        {
            'jsonldPredicate': '@id',
            'type': 'string',
            'name': 'id'
        },
        {
            'type': [
                'null',
                'boolean'
            ],
            'name': 'publish',
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field1',
            'aetherMaskingLevel': 1
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field2',
            'aetherMaskingLevel': 2
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field3',
            'aetherMaskingLevel': 3
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field4',
            'aetherMaskingLevel': 4
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field5',
            'aetherMaskingLevel': 5
        },
    ],
    'type': 'record',
    'name': 'TestBooleanPass'
}

schema_enum_pass = {
    'fields': [
        {
            'jsonldPredicate': '@id',
            'type': 'string',
            'name': 'id'
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'publish',
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field1',
            'aetherMaskingLevel': 1
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field2',
            'aetherMaskingLevel': 2
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field3',
            'aetherMaskingLevel': 3
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field4',
            'aetherMaskingLevel': 4
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field5',
            'aetherMaskingLevel': 5
        },
    ],
    'type': 'record',
    'name': 'TestEnumPass'
}

schema_top_secret = {
    'fields': [
        {
            'jsonldPredicate': '@id',
            'type': 'string',
            'name': 'id'
        },
        {
            'type': [
                'null',
                'boolean'
            ],
            'name': 'publish',
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field1',
            'aetherMaskingLevel': 'public'
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field2',
            'aetherMaskingLevel': 'confidential'
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field3',
            'aetherMaskingLevel': 'secret'
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field4',
            'aetherMaskingLevel': 'top secret'
        },
        {
            'type': [
                'null',
                'string'
            ],
            'name': 'field5',
            'aetherMaskingLevel': 'ufos'
        },
    ],
    'type': 'record',
    'name': 'TestTopSecret'
}


def mock_message_schema_boolean_pass(count=10, *args, **kwargs):
    from uuid import uuid4
    messages = []
    for x in range(count):
        msg = {
            'id': str(uuid4()),
            'publish': False
        }
        for x in range(1, 6):
            msg['field%s' % x] = str(uuid4())
        messages.append(msg)
    if not count % 2 == 0:
        raise ValueError('Count needs to be even')

    for x in range(int(count / 2)):
        messages[x]['publish'] = True
    return messages


def mock_message_schema_enum_pass(count=10, *args, **kwargs):
    messages = mock_message_schema_boolean_pass(count=count)
    for x, msg in enumerate(messages, 0):
        if x % 3 == 0:
            msg['publish'] = 'maybe'
        elif x % 2 == 0:
            msg['publish'] = 'yes'
        else:
            msg['publish'] = 'no'
    return messages


test_schemas = {
    'TestBooleanPass': {
        'schema': schema_boolean_pass,
        'mocker': mock_message_schema_boolean_pass
    },
    'TestEnumPass': {
        'schema': schema_enum_pass,
        'mocker': mock_message_schema_enum_pass
    },
    'TestTopSecret': {
        'schema': schema_top_secret,
        'mocker': mock_message_schema_boolean_pass
    }
}
