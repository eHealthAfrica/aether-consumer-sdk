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

from copy import copy
import types

import requests

from . import *  # noqa
from aet.job import JobStatus
from aet.logger import get_logger
from aet.kafka import KafkaConsumer, FilterConfig, MaskConfig
from aether.python.redis.task import LOG as task_log

from aet.logger import wrap_logger

wrap_logger(task_log, 'TASK')

LOG = get_logger('TestSDK')

# from aet.kafka import KafkaConsumer

TENANT = 'test'

# Test Suite contains both unit and integration tests
# Unit tests can be run on their own from the root directory
# enter the bash environment for the version of python you want to test
# for example for python 3
# `docker-compose run consumer-sdk-test bash`
# then start the unit tests with
# `pytest -m unit`


######
#
#  SETTINGS TESTS
#
#####

@pytest.mark.unit
def test_settings(fake_settings):
    cset = fake_settings
    settings_copy = cset.copy()
    assert('C' in cset)  # exclude doesn't matter in orignial
    assert('B' not in settings_copy)  # exclude works on copies
    assert(settings_copy.get('D') == 2)  # alias works on both
    assert(cset.get('D') == 2)  # alias works on both
    assert(cset.get('MISSING', 3) == 3)


@pytest.mark.unit
def test_settings_check(fake_settings):
    with pytest.raises(AssertionError):
        settings.check_required_fields(fake_settings, '["A", "B", "E"]')
    assert('A' in settings.check_required_fields(fake_settings, '["A", "B", "C"]'))


######
#
#  KAFKA TESTS
#
#####
@pytest.mark.kafka
@pytest.mark.parametrize("messages,topic,is_json", [
    (pytest.lazy_fixture('messages_test_json_utf8'), 'TestJSONMessagesUTF', True),
    (pytest.lazy_fixture('messages_test_json_ascii'), 'TestJSONMessagesASCII', True),
    (pytest.lazy_fixture('messages_test_text_ascii'), 'TestPlainMessagesASCII', False),
    (pytest.lazy_fixture('messages_test_text_utf8'), 'TestPlainMessagesUTF', False)
])
def test_read_messages_no_schema(messages, topic, is_json, default_consumer_args):
    _ids = [m['id'] for m in messages]
    # topic = "TestPlainMessages"
    assert(len(messages) ==
           topic_size), "Should have generated the right number of messages"
    iter_consumer = KafkaConsumer(**default_consumer_args)
    while not iter_consumer.list_topics():
        print('waiting for kafka to populate')
        sleep(5)
    iter_consumer.subscribe([topic])
    # iter_consumer.seek_to_beginning()
    # more than a few hundres is too large to grab in one pass when not serialized
    for x in range(int(10 * topic_size / 500)):
        messages = iter_consumer.poll_and_deserialize(timeout=3, num_messages=500)
        for msg in messages:
            if is_json:
                _id = msg.value.get('id')
            else:
                _id = msg.value
            try:
                # remove found records from our pick list
                _ids.remove(_id)
            except ValueError:
                # record may be in another batch
                pass
        # read messages and check masking
        if len(_ids) == 0:
            break
    iter_consumer.close()
    # make sure we read all the records
    assert(len(_ids) == 0)


@pytest.mark.kafka
@pytest.mark.parametrize("emit_level,unmasked_fields", [
    (0, 2),
    (1, 3),
    (2, 4),
    (3, 5),
    (4, 6),
    (5, 7),
])
def test_masking_boolean_pass(default_consumer_args,
                              messages_test_boolean_pass,
                              emit_level,
                              unmasked_fields):
    topic = "TestBooleanPass"
    assert(len(messages_test_boolean_pass) ==
           topic_size), "Should have generated the right number of messages"
    # set configs
    consumer_kwargs = default_consumer_args
    consumer_kwargs["aether_masking_schema_emit_level"] = emit_level
    # get messages for this emit level
    iter_consumer = KafkaConsumer(**consumer_kwargs)
    iter_consumer.subscribe([topic])
    iter_consumer.seek_to_beginning()
    messages = iter_consumer.poll_and_deserialize(timeout=5, num_messages=1000)
    iter_consumer.close()
    # read messages and check masking
    for msg in messages:
        assert(len(msg.value.keys()) ==
               unmasked_fields), "%s fields should be unmasked" % unmasked_fields


@pytest.mark.kafka
@pytest.mark.parametrize("emit_level,unmasked_fields,override", [
    ("uncategorized", 2, None),
    ("public", 3, None),
    ("confidential", 4, None),
    ("secret", 5, None),
    ("top secret", 6, None),
    ("ufos", 7, None),
    ("uncategorized", 7, "ufos"),
])
@pytest.mark.parametrize("masking_taxonomy", [
    (["public", "confidential", "secret", "top secret", "ufos"])
])
def test_masking_category_pass(default_consumer_args,
                               messages_test_secret_pass,
                               emit_level,
                               masking_taxonomy,
                               unmasked_fields,
                               override):
    topic = "TestTopSecret"
    assert(len(messages_test_secret_pass) ==
           topic_size), "Should have generated the right number of messages"
    # set configs
    consumer_kwargs = default_consumer_args
    consumer_kwargs["aether_masking_schema_emit_level"] = emit_level
    consumer_kwargs["aether_masking_schema_levels"] = masking_taxonomy
    # get messages for this emit level
    iter_consumer = KafkaConsumer(**consumer_kwargs)
    iter_consumer.subscribe([topic])
    if override:
        _config = MaskConfig(
            mask_query=consumer_kwargs['aether_masking_schema_annotation'],
            mask_levels=masking_taxonomy,
            emit_level=override
        )
        iter_consumer.set_topic_mask_config(topic, _config)
    messages = iter_consumer.poll_and_deserialize(timeout=5, num_messages=1000)
    iter_consumer.close()
    # read messages and check masking
    for msg in messages:
        assert(len(msg.value.keys()) ==
               unmasked_fields), "%s fields should be unmasked" % unmasked_fields


@pytest.mark.kafka
@pytest.mark.parametrize("required_field, publish_on, expected_count", [
    (True, [True], int(topic_size / 2)),
    (True, [False], int(topic_size / 2)),
    (True, [True, False], topic_size),
    (True, True, int(topic_size / 2)),
    (True, False, int(topic_size / 2)),
    (False, True, int(topic_size))  # Turn off publish filtering
])
def test_publishing_boolean_pass(default_consumer_args,
                                 messages_test_boolean_pass,
                                 required_field,
                                 publish_on,
                                 expected_count):
    topic = "TestBooleanPass"
    assert(len(messages_test_boolean_pass) ==
           topic_size), "Should have generated the right number of messages"
    # set configs
    consumer_kwargs = default_consumer_args
    consumer_kwargs["aether_emit_flag_required"] = required_field
    consumer_kwargs["aether_emit_flag_values"] = publish_on
    # get messages for this emit level
    iter_consumer = KafkaConsumer(**consumer_kwargs)
    iter_consumer.subscribe([topic])
    iter_consumer.seek_to_beginning()
    messages = iter_consumer.poll_and_deserialize(timeout=10, num_messages=1000)
    iter_consumer.close()
    # read messages and check masking
    count = 0
    for msg in messages:
        count += 1
    assert(count == expected_count), "unexpected # of messages published"


@pytest.mark.kafka
@pytest.mark.parametrize("publish_on, expected_values, override", [
    (["yes"], ["yes"], None),
    (["yes", "maybe"], ["yes", "maybe"], None),
    ("yes", ["yes"], None),
    (["yes"], ["yes", "maybe"], ["yes", "maybe"]),
])
def test_publishing_enum_pass(default_consumer_args,
                              messages_test_enum_pass,
                              publish_on,
                              expected_values,
                              override):
    topic = "TestEnumPass"
    assert(len(messages_test_enum_pass) ==
           topic_size), "Should have generated the right number of messages"
    # set configs
    consumer_kwargs = default_consumer_args
    consumer_kwargs["aether_emit_flag_values"] = publish_on
    # get messages for this emit level
    iter_consumer = KafkaConsumer(**consumer_kwargs)
    iter_consumer.subscribe([topic])
    iter_consumer.seek_to_beginning()
    if override:
        _config = FilterConfig(
            check_condition_path=consumer_kwargs['aether_emit_flag_field_path'],
            pass_conditions=override,
            requires_approval=True
        )
        iter_consumer.set_topic_filter_config(topic, _config)
    messages = iter_consumer.poll_and_deserialize(timeout=5, num_messages=1000)
    iter_consumer.close()
    # read messages and check masking
    for msg in messages:
        assert(msg.value.get("publish") in expected_values)


@pytest.mark.unit
@pytest.mark.parametrize("field_path,field_value,pass_msg,fail_msg", [
    (None, None, {"approved": True}, {"approved": False}),
    ("$.checked", None, {"checked": True}, {"checked": False}),
    (None, [False], {"approved": False}, {"approved": True}),
    (None, ["yes", "maybe"], {"approved": "yes"}, {"approved": "no"}),
    (None, ["yes", "maybe"], {"approved": "maybe"}, {"approved": "no"}),
    (None, ["yes", "maybe"], {"approved": "maybe"}, {"checked": "maybe"})
])
def test_get_approval_filter(offline_consumer, field_path, field_value, pass_msg, fail_msg):
    if field_path:
        offline_consumer._add_config({"aether_emit_flag_field_path": field_path})
    if field_value:
        offline_consumer._add_config({"aether_emit_flag_values": field_value})
    config = offline_consumer._default_filter_config()
    _filter = offline_consumer.get_approval_filter(config)
    assert(_filter(pass_msg))
    assert(_filter(fail_msg) is not True)


@pytest.mark.unit
def test_message_deserialize__failure(offline_consumer):
    msg = 'a utf-16 string'.encode('utf-16')
    obj = io.BytesIO()
    obj.write(msg)
    with pytest.raises(UnicodeDecodeError):
        msg = offline_consumer._decode_text(obj)
    assert(True)


@pytest.mark.unit
@pytest.mark.parametrize("emit_level", [
    (0),
    (1),
    (2),
    (3),
    (4),
    (5)
])
@pytest.mark.unit
def test_msk_msg_default_map(offline_consumer, sample_schema, sample_message, emit_level):
    offline_consumer._add_config({"aether_masking_schema_emit_level": emit_level})
    config = offline_consumer._default_mask_config()
    mask = offline_consumer.get_mask_from_schema(sample_schema, config)
    masked = mask(sample_message)
    assert(len(masked.keys()) == (emit_level + 2)), ("%s %s" % (emit_level, masked))


@pytest.mark.unit
@pytest.mark.parametrize("emit_level,expected_count", [
    ("no matching taxonomy", 2),
    ("public", 3),
    ("confidential", 4),
    ("secret", 5),
    ("top secret", 6),
    ("ufos", 7)
])
@pytest.mark.parametrize("possible_levels", [([  # Single parameter for all tests
    "public",
    "confidential",
    "secret",
    "top secret",
    "ufos"
])])
def test_msk_msg_custom_map(offline_consumer,
                            sample_schema_top_secret,
                            sample_message_top_secret,
                            emit_level,
                            possible_levels,
                            expected_count):
    offline_consumer._add_config({"aether_masking_schema_emit_level": emit_level})
    offline_consumer._add_config({"aether_masking_schema_levels": possible_levels})
    config = offline_consumer._default_mask_config()
    mask = offline_consumer.get_mask_from_schema(sample_schema_top_secret, config)
    masked = mask(sample_message_top_secret)
    assert(len(masked.keys()) == (expected_count)), ("%s %s" % (emit_level, masked))


######
#
#  Resource Tests
#
#####

@pytest.mark.unit
def test_bad_resource_missing_attrs():
    with pytest.raises(TypeError):
        BadResource()
    with pytest.raises(TypeError):
        BadJob()


@pytest.mark.unit
def test_resource__basic_validate_pass():
    assert(TestResource._validate_pretty(TestResourceDef1))


@pytest.mark.unit
def test_resource__basic_validate_fail():
    assert(TestResource._validate({}) is not True)


@pytest.mark.unit
def test_resource__basic_validate_fail_verbose():
    res = TestResource._validate_pretty({})
    assert(res['valid'] is not True)
    assert(len(res['validation_errors']) > 0)


@pytest.mark.unit
def test_resource__basic_method():
    tenant = 'test-1'
    res = TestResource(tenant, TestResourceDef1, None)
    assert(res.upper('username') == 'USER')


@pytest.mark.unit
def test_resource__basic_describe_static():
    assert(len(TestResource._describe_static()) == 3)


@pytest.mark.unit
def test_resource__basic_describe_public():
    _desc = TestResource._describe()
    assert(len(_desc) == 10)
    expected = ['get', 'add', 'delete']
    for i in _desc:
        assert(type(i) is MethodDesc)
        try:
            expected.remove(i.method)
        except ValueError:
            pass
    json.dumps(_desc)
    assert(len(expected) == 0)


@pytest.mark.unit
def test_resource__mask_config():
    config = {
        'id': 'an_id',
        'password': 'something'
    }
    res = TestResource._mask_config(config)
    assert(res['password'] != 'something')
    assert(res['id'] == 'an_id')


######
#
#  Jsonpath Tests
#
#####

cached_parser_msg = {
    'a': 1,
    'b': [
        {
            'a': 1,
            'b': 2,
        },
        {
            'a': 2,
            'b': 3,
        },
        {
            'a': 2,
            'b': 3,
        }
    ],
}


@pytest.mark.unit
@pytest.mark.parametrize("path,match_count", [
    ('$.a', 1),
    ('$.b', 1),
    ('$.b[*]', 3),
    ('$.b[*].a', 3),
    ('$.b[?a = 1].b', 1),
    ('$.b[?a > 1].b', 2),
    ('$.b[?a = 2].b', 2),
    ('$.b[?a = 3].b', 0)  # no match, but valid syntax
])
def test_cached_parser__success(path, match_count):
    matches = CachedParser.find(path, cached_parser_msg)
    assert(len(matches) == match_count)


@pytest.mark.unit
def test_cached_parser__bad_path():
    path = '$.b[this_is_nonsense].b/5^&##'  # invalid syntax
    with pytest.raises(Exception):
        CachedParser.find(path, cached_parser_msg)
    assert(True)


######
#
#  API TESTS
#
#####

@pytest.mark.unit
@pytest.mark.parametrize("call,result,raises_error", [
                        ('job/describe', -1, False),
                        ('job/delete?id=fake', False, False),
                        ('job/delete', None, True),
                        ('job/get?id=fake', None, True),
                        ('job/get', None, True),
                        ('job/list', [], False),
                        ('job/get_status?id=fake', {'error': 'job object with id : fake not found'}, True),
                        ('resource/delete?id=fake', False, False),
                        ('resource/delete', None, True),
                        ('resource/get', None, True),
                        ('resource/get?id=fake', None, True),
                        ('resource/list', [], False),
                        ('resource/validate_pretty', -1, False),
                        ('resource/null?id=fake', -1, True),  # properly dispatched to missing JM
                        ('bad_resource/list', {}, True),
                        ('health', 'healthy', False)
])
def test_api_get_calls(call, result, raises_error, mocked_api):
    user = settings.CONSUMER_CONFIG.get('ADMIN_USER')
    pw = settings.CONSUMER_CONFIG.get('ADMIN_PW')
    auth = requests.auth.HTTPBasicAuth(user, pw)
    port = settings.CONSUMER_CONFIG.get('EXPOSE_PORT')
    url = f'http://localhost:{port}/{call}'
    res = requests.get(url, auth=auth)
    try:
        res.raise_for_status()
    except Exception as aer:
        LOG.debug(aer)
        assert(raises_error), res.content
        return
    try:
        val = res.json()
    except json.decoder.JSONDecodeError:
        val = res.text
    finally:
        if result is not -1:
            # want to be able to expect and ignore complex results
            assert(val == result)


@pytest.mark.unit
def test_api__bad_resource_type(mocked_api):
    user = settings.CONSUMER_CONFIG.get('ADMIN_USER')
    pw = settings.CONSUMER_CONFIG.get('ADMIN_PW')
    auth = requests.auth.HTTPBasicAuth(user, pw)
    port = settings.CONSUMER_CONFIG.get('EXPOSE_PORT')
    url = f'http://localhost:{port}/super_bad_resource/list'
    res = requests.get(url, auth=auth)
    with pytest.raises(requests.exceptions.HTTPError):
        res.raise_for_status()
    assert(res.status_code == 404)


@pytest.mark.unit
def test_api__bad_auth(mocked_api):
    user = settings.CONSUMER_CONFIG.get('ADMIN_USER')
    pw = 'bad_password'
    auth = requests.auth.HTTPBasicAuth(user, pw)
    port = settings.CONSUMER_CONFIG.get('EXPOSE_PORT')
    url = f'http://localhost:{port}/job/get?id=someid'
    res = requests.get(url, auth=auth)
    with pytest.raises(requests.exceptions.HTTPError):
        res.raise_for_status()
    assert(res.status_code == 401)


@pytest.mark.unit
def test_api__allowed_types(mocked_api):
    crud = ['READ', 'CREATE', 'DELETE', 'LIST', 'VALIDATE']
    job_only = ['pause', 'resume', 'get_status']
    _allowed_types: Dict[str, List] = {
        'job': crud + job_only,
        'resource': crud
    }
    for name, allowed_ops in _allowed_types.items():
        assert(name) in mocked_api._allowed_types.keys()
        for op in allowed_ops:
            assert(op in mocked_api._allowed_types[name])


@pytest.mark.unit
@pytest.mark.parametrize("call,result,json_body,raises_error", [
                        ('job/add', False, {'a': 'b'}, True),
                        ('job/update', False, {'a': 'b'}, True),
                        ('job/add', True, {}, True),
                        ('job/update', True, {}, True),
                        ('job/validate', {'valid': False}, {'id': 'someid'}, False),
                        ('job/pause?id=someid', {'valid': False}, {}, True),
                        ('resource/add', False, {'a': 'b'}, True),
                        ('resource/update', False, {'a': 'b'}, True),
                        ('resource/add', True, {'id': 'someid', 'username': 'user', 'password': 'pw1'}, False),
                        ('resource/update', True, {'id': 'someid', 'username': 'user', 'password': 'pw2'}, False),
                        ('resource/get?id=someid', lambda x: x['password'] == '*****', {}, False),
                        ('resource/status', None, {'id': 'someid'}, True),  # these are now allowed
                        ('resource/pause', None, {'id': 'someid'}, True),
                        ('resource/resume', None, {'id': 'someid'}, True),
])
def test_api_post_calls(call, result, json_body, raises_error, mocked_api):
    user = settings.CONSUMER_CONFIG.get('ADMIN_USER')
    pw = settings.CONSUMER_CONFIG.get('ADMIN_PW')
    auth = requests.auth.HTTPBasicAuth(user, pw)
    port = settings.CONSUMER_CONFIG.get('EXPOSE_PORT')
    url = f'http://localhost:{port}/{call}'
    res = requests.post(url, auth=auth, json=json_body)
    try:
        res.raise_for_status()
    except Exception:
        assert(raises_error), res.content
        return
    try:
        val = res.json()
    except json.decoder.JSONDecodeError:
        val = res.text
    finally:
        if not isinstance(result, types.FunctionType):
            assert(val == result), f'{call} | {result} | {json_body}'
        else:
            assert(result(val)), val


######
#
#  Job Manager Tests
#
#####

i_resources = [
    {
        'id': 'a1',
        'say': 'a',
        'wait': 1
    },
    {
        'id': 'b1',
        'say': 'b',
        'wait': 1
    },
    {
        'id': 'c1',
        'say': 'c',
        'wait': 1
    }
]

i_job = {
    'id': 'job-id',
    'resources': ['a1', 'b1', 'c1'],
    'poll_interval': 1
}

tenant = 'test-1'
_type = 'resource'


@pytest.mark.unit
def test_itest_add_resources(IJobManagerFNScope):
    for res in i_resources:
        IJobManagerFNScope.task.add(res, _type, tenant)
    # LOG.debug(list(IJobManagerFNScope.task.list(_type)))
    # for i in IJobManagerFNScope.task.redis.scan_iter('*'):
    #     LOG.debug(i.decode('utf-8'))
    sleep(1)
    _ids = [r.get('id') for r in i_resources]
    keys = [key for key in IJobManagerFNScope.resources.instances.keys()]
    for _id in _ids:
        assert(any([_id in key for key in keys])), [_ids, keys]


@pytest.mark.unit
def test_itest_remove__readd_resources(IJobManagerFNScope):
    # remove the resources
    for res in i_resources:
        _id = res.get('id')
        IJobManagerFNScope.task.remove(_id, _type, tenant)
    sleep(1)
    _ids = list(IJobManagerFNScope.resources.instances.keys())
    assert(len(_ids) == 0)
    # add them again
    for res in i_resources:
        IJobManagerFNScope.task.add(res, _type, tenant)
    sleep(1)
    _ids = [r.get('id') for r in i_resources]
    for _id in _ids:
        assert(any([_id in key for key in IJobManagerFNScope.resources.instances.keys()]))


@pytest.mark.unit
def test_itest_reinit_resources_add_job(IJobManagerFNScope):
    _ids = [r.get('id') for r in i_resources]
    keys = list(IJobManagerFNScope.resources.instances.keys())
    for _id in _ids:
        assert(any([_id in key for key in keys]))
    IJobManagerFNScope.task.add(i_job, 'job', tenant)
    sleep(1)
    _id = i_job.get('id')
    assert(any([_id in key for key in IJobManagerFNScope.jobs.keys()]))


@pytest.mark.unit
def test_itest_reinit_all_control_job(IJobManagerFNScope):
    _id = 'job-id'
    key = f'{tenant}:{_id}'
    man = IJobManagerFNScope
    job = man.jobs[key]
    job.pause()
    assert(job.status == JobStatus.PAUSED)
    job.resume()
    assert(job.status == JobStatus.NORMAL)
    res = job.get_status()
    assert(res is not None)


@pytest.mark.unit
def test_all_artifacts_survive_restart(IJobManager):
    sleep(5)
    _ids = [r.get('id') for r in i_resources]
    keys = list(IJobManager.resources.instances.keys())
    for _id in _ids:
        assert(any([_id in key for key in keys]))
    _id = i_job.get('id')
    assert(any([_id in key for key in IJobManager.jobs.keys()]))


@pytest.mark.unit
def test_itest_modify_job(IJobManager):
    new_def = copy(i_job)
    new_def['poll_interval'] = 2
    IJobManager.task.add(new_def, 'job', tenant)
    sleep(1)
    _id = i_job.get('id')
    key = [key for key in IJobManager.jobs.keys() if _id in key][0]
    job = IJobManager.jobs[key]
    assert(job.config.get('poll_interval') == 2)


@pytest.mark.unit
def test_itest_remove_job(IJobManager):
    _id = i_job.get('id')
    IJobManager.task.remove(_id, 'job', tenant)
    sleep(1)
    keys = list(IJobManager.jobs.keys())
    assert(not any([_id in key for key in keys]))


@pytest.mark.unit
def test_itest_handle_exception_and_resume_job(IJobManager):
    _id = i_job.get('id')
    IJobManager.task.add(i_job, 'job', tenant)
    sleep(1)
    keys = list(IJobManager.jobs.keys())
    assert(any([_id in key for key in keys]))
    job_id = f'{tenant}:{_id}'
    job = IJobManager.jobs[job_id]
    job._cause_exception()
    sleep(3)
    assert(job.get_status() == str(JobStatus.DEAD))
    job._revert_exception()
    job.resume()
    sleep(3)
    assert(job.get_status() == str(JobStatus.NORMAL))
    IJobManager.task.remove(_id, 'job', tenant)
    sleep(1)
    keys = list(IJobManager.jobs.keys())
    assert(not any([_id in key for key in keys]))


@pytest.mark.unit
def test_itest_add_ten_jobs(IJobManager):
    assert(IJobManager.task is not None)
    names = []
    for x in range(10):
        name = f'job-{x}'
        _job = copy(i_job)
        _job['id'] = name
        names.append(name)
        IJobManager.task.add(_job, 'job', tenant)
    sleep(1)
    _ids = IJobManager.list_jobs(tenant)
    LOG.debug(_ids)
    for name in names:
        assert(name in _ids)
    last_log = None
    for job in IJobManager.jobs.values():
        assert(job.status is JobStatus.NORMAL)
        logs = job.get_logs()
        assert(logs != last_log)
        last_log = logs


@pytest.mark.unit
def test_itest_modify_resources(IJobManager):
    new_value = 2
    for res in i_resources:
        doc = copy(res)
        doc['wait'] = new_value
        IJobManager.task.add(doc, _type, tenant)
    sleep(2)
    for job in IJobManager.jobs.values():
        assert(job.status is JobStatus.NORMAL)
        value = job.share_resource('a1', 'wait')
        assert(value == new_value)
