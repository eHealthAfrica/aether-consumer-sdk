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

import requests

from . import *  # noqa
from aet.job import BaseJob, JobStatus
from aet.kafka import KafkaConsumer

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
@pytest.mark.integration
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
    iter_consumer.subscribe(topic)
    iter_consumer.seek_to_beginning()
    # more than a few hundres is too large to grab in one pass when not serialized
    for x in range(int(3 * topic_size / 500)):
        messages = iter_consumer.poll_and_deserialize(timeout_ms=10000, max_records=1000)
        # read messages and check masking
        for partition, packages in messages.items():
            for package in packages:
                for msg in package.get("messages"):
                    if is_json:
                        _id = msg.get('id')
                    else:
                        _id = msg
                    try:
                        # remove found records from our pick list
                        _ids.remove(_id)
                    except ValueError:
                        # record may be in another batch
                        pass
        if len(_ids) == 0:
            break
    iter_consumer.close()
    # make sure we read all the records
    assert(len(_ids) == 0)


@pytest.mark.integration
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
    iter_consumer.subscribe(topic)
    iter_consumer.seek_to_beginning()
    messages = iter_consumer.poll_and_deserialize(timeout_ms=10000, max_records=1000)
    iter_consumer.close()
    # read messages and check masking
    for partition, packages in messages.items():
        for package in packages:
            for msg in package.get("messages"):
                assert(len(msg.keys()) ==
                       unmasked_fields), "%s fields should be unmasked" % unmasked_fields


@pytest.mark.integration
@pytest.mark.parametrize("emit_level,unmasked_fields", [
    ("uncategorized", 2),
    ("public", 3),
    ("confidential", 4),
    ("secret", 5),
    ("top secret", 6),
    ("ufos", 7),
])
@pytest.mark.parametrize("masking_taxonomy", [
    (["public", "confidential", "secret", "top secret", "ufos"])
])
def test_masking_category_pass(default_consumer_args,
                               messages_test_secret_pass,
                               emit_level,
                               masking_taxonomy,
                               unmasked_fields):
    topic = "TestTopSecret"
    assert(len(messages_test_secret_pass) ==
           topic_size), "Should have generated the right number of messages"
    # set configs
    consumer_kwargs = default_consumer_args
    consumer_kwargs["aether_masking_schema_emit_level"] = emit_level
    consumer_kwargs["aether_masking_schema_levels"] = masking_taxonomy
    # get messages for this emit level
    iter_consumer = KafkaConsumer(**consumer_kwargs)
    iter_consumer.subscribe(topic)
    iter_consumer.seek_to_beginning()
    messages = iter_consumer.poll_and_deserialize(timeout_ms=10000, max_records=1000)
    iter_consumer.close()
    # read messages and check masking
    for partition, packages in messages.items():
        for package in packages:
            for msg in package.get("messages"):
                assert(len(msg.keys()) ==
                       unmasked_fields), "%s fields should be unmasked" % unmasked_fields


@pytest.mark.integration
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
    iter_consumer.subscribe(topic)
    iter_consumer.seek_to_beginning()
    messages = iter_consumer.poll_and_deserialize(timeout_ms=10000, max_records=1000)
    iter_consumer.close()
    # read messages and check masking
    count = 0
    for partition, packages in messages.items():
        for package in packages:
            for msg in package.get("messages"):
                count += 1
    assert(count == expected_count), "unexpected # of messages published"


@pytest.mark.integration
@pytest.mark.parametrize("publish_on, expected_values", [
    (["yes"], ["yes"]),
    (["yes", "maybe"], ["yes", "maybe"]),
    ("yes", ["yes"])
])
def test_publishing_enum_pass(default_consumer_args,
                              messages_test_enum_pass,
                              publish_on,
                              expected_values):
    topic = "TestEnumPass"
    assert(len(messages_test_enum_pass) ==
           topic_size), "Should have generated the right number of messages"
    # set configs
    consumer_kwargs = default_consumer_args
    consumer_kwargs["aether_emit_flag_values"] = publish_on
    # get messages for this emit level
    iter_consumer = KafkaConsumer(**consumer_kwargs)
    iter_consumer.subscribe(topic)
    iter_consumer.seek_to_beginning()
    messages = iter_consumer.poll_and_deserialize(timeout_ms=10000, max_records=1000)
    iter_consumer.close()
    # read messages and check masking
    for partition, packages in messages.items():
        for package in packages:
            for msg in package.get("messages"):
                assert(msg.get("publish") in expected_values)


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
    _filter = offline_consumer.get_approval_filter()
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
    mask = offline_consumer.get_mask_from_schema(sample_schema)
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
    mask = offline_consumer.get_mask_from_schema(sample_schema_top_secret)
    masked = mask(sample_message_top_secret)
    assert(len(masked.keys()) == (expected_count)), ("%s %s" % (emit_level, masked))


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
                        ('job/delete?id=fake', True, False),
                        ('job/delete', None, True),
                        ('job/get?id=fake', {}, False),
                        ('job/get', None, True),
                        ('job/list', [], False),
                        ('resource/delete?id=fake', True, False),
                        ('resource/delete', None, True),
                        ('resource/get', None, True),
                        ('resource/get?id=fake', {}, True),
                        ('resource/list', [], False),
                        ('bad_resource/list', {}, True),
                        ('healthcheck', 'healthy', False)
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
    except Exception:
        assert(raises_error)
        return
    try:
        val = res.json()
    except json.decoder.JSONDecodeError:
        val = res.text
    finally:
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
    job_only = ['PAUSE', 'RESUME', 'STATUS']
    _allowed_types: Dict[str, List] = {
        'job': crud + job_only,
        'resource': crud
    }
    for name, allowed_ops in _allowed_types.items():
        assert(name) in mocked_api._allowed_types.keys()
        for op in allowed_ops:
            assert(op in mocked_api._allowed_types[name])


@pytest.mark.unit
@pytest.mark.parametrize("call,result,body,raises_error", [
                        ('job/add', False, {'a': 'b'}, False),
                        ('job/update', False, {'a': 'b'}, False),
                        ('job/add', True, {}, False),
                        ('job/update', True, {}, False),
                        ('job/status', [], {'id': 'someid'}, False),
                        ('job/pause', True, {'id': 'someid'}, False),
                        ('job/resume', True, {'id': 'someid'}, False),
                        ('job/validate', {'valid': False}, {'id': 'someid'}, False),
                        ('resource/add', False, {'a': 'b'}, False),
                        ('resource/update', False, {'a': 'b'}, False),
                        ('resource/add', True, {}, False),
                        ('resource/update', True, {}, False),
                        ('resource/status', None, {'id': 'someid'}, True),  # these are now allowed
                        ('resource/pause', None, {'id': 'someid'}, True),
                        ('resource/resume', None, {'id': 'someid'}, True),
])
def test_api_post_calls(call, result, body, raises_error, mocked_api):
    user = settings.CONSUMER_CONFIG.get('ADMIN_USER')
    pw = settings.CONSUMER_CONFIG.get('ADMIN_PW')
    auth = requests.auth.HTTPBasicAuth(user, pw)
    port = settings.CONSUMER_CONFIG.get('EXPOSE_PORT')
    url = f'http://localhost:{port}/{call}'
    res = requests.post(url, auth=auth, json=body)
    try:
        res.raise_for_status()
    except Exception:
        assert(raises_error)
        return
    try:
        val = res.json()
    except json.decoder.JSONDecodeError:
        val = res.text
    finally:
        assert(val == result), f'{call} | {result} | {body}'


######
#
#  CONSUMER TESTS
#
#####

@pytest.mark.integration
def test_consumer__startup_shutdown(consumer):
    call = 'healthcheck'
    user = settings.CONSUMER_CONFIG.get('ADMIN_USER')
    pw = settings.CONSUMER_CONFIG.get('ADMIN_PW')
    auth = requests.auth.HTTPBasicAuth(user, pw)
    port = consumer.consumer_settings.get('EXPOSE_PORT')
    url = f'http://localhost:{port}/{call}'
    res = requests.get(url, auth=auth)
    res.raise_for_status()
    assert(res.text == 'healthy')


@pytest.mark.integration
def test_consumer__job_registration(consumer: BaseConsumer):
    redis_subscribe_delay = 0.25
    _id = '001-01'
    job_id = f'{TENANT}:{_id}'
    job_def = {'id': _id, 'purpose': 'counter'}
    consumer.task.add(job_def, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    assert(job_id in consumer.job_manager.jobs.keys())
    _job: BaseJob = consumer.job_manager.jobs[job_id]
    _job.status = JobStatus.PAUSED
    sleep(redis_subscribe_delay)
    # check that it stays paused
    assert(_job.status is JobStatus.PAUSED)
    _job.status = JobStatus.NORMAL
    assert('purpose' in _job.config.keys()), f'missing: {_job.config}'
    assert(_job.config['purpose'] == 'counter')
    assert(isinstance(_job, BaseJob))
    old_val = _job.value
    sleep(0.5)
    assert(_job.value > old_val)
    new_purpose = 'some new purpose'
    job_def['purpose'] = new_purpose
    consumer.task.add(job_def, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    assert(_job.config['purpose'] == new_purpose)
    _job._cause_exception()
    sleep(redis_subscribe_delay)
    assert(_job.status is JobStatus.DEAD)
    removed = consumer.task.remove(_id, type='job', tenant=TENANT)
    assert(removed is True)
    sleep(redis_subscribe_delay)
    assert(job_id not in consumer.job_manager.jobs.keys())


@pytest.mark.integration
def test_consumer__job_control(consumer: BaseConsumer):
    redis_subscribe_delay = 0.25
    _id = '001-02'
    job_id = f'{TENANT}:{_id}'
    job_def = {'id': _id, 'purpose': 'counter'}
    consumer.task.add(job_def, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    # check normal status
    assert(_id in list(consumer.task.list(type='job', tenant=TENANT)))
    _job: BaseJob = consumer.job_manager.jobs[job_id]
    assert(_job.status is JobStatus.NORMAL)
    status = consumer.status(_id, TENANT)
    assert('JobStatus.NORMAL' in status)
    # pause job and check status
    ok = consumer.pause(_id, TENANT)
    assert(ok)
    sleep(redis_subscribe_delay)
    status = consumer.status(_id, TENANT)
    assert('JobStatus.PAUSED' in status)
    # resume and check status
    ok = consumer.resume(_id, TENANT)
    assert(ok)
    sleep(redis_subscribe_delay)
    status = consumer.status(_id, TENANT)
    assert('JobStatus.NORMAL' in status)
    statuses = consumer.status([_id, _id], TENANT)
    assert(isinstance(statuses, list))
    for status in statuses:
        assert(status == 'JobStatus.NORMAL')
    # crash the job and check status
    _job._cause_exception()
    sleep(redis_subscribe_delay)
    status = consumer.status(_id, TENANT)
    assert('JobStatus.DEAD' in status)
    consumer.task.remove(_id, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    assert(_id not in consumer.job_manager.jobs.keys())


@pytest.mark.integration
def test_consumer__job_control_failures(consumer: BaseConsumer):
    bad_id = 'a-bad-id'
    ok = consumer.pause(bad_id, TENANT)
    assert(ok is False)
    ok = consumer.resume(bad_id, TENANT)
    assert(ok is False)
    status = consumer.status(bad_id, TENANT)
    assert(isinstance(status[0], str))


@pytest.mark.integration
def test_consumer__job_registration_with_resource(consumer: BaseConsumer):
    redis_subscribe_delay = 0.25
    res_def = {
        'id': 'res-001',
        'value': 1000000
    }
    _id = '002'
    job_id = f'{TENANT}:{_id}'
    job_def = {
        'id': _id,
        'purpose': 'counter',
        'resources': 'res-001'
    }
    consumer.task.add(res_def, type='resource', tenant=TENANT)
    sleep(redis_subscribe_delay)
    consumer.task.add(job_def, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    assert(job_id in list(consumer.job_manager.jobs.keys()))
    _job: BaseJob = consumer.job_manager.jobs[job_id]
    assert('purpose' in _job.config.keys()), f'missing: {_job.config}'
    assert(_job.config['purpose'] == 'counter')
    assert(_job.resources['resource'].get('value') == res_def['value'])
    res_def['value'] = 1000002
    consumer.task.add(res_def, type='resource', tenant=TENANT)
    sleep(redis_subscribe_delay)
    # check that the value was updated
    assert(_job.resources['resource'].get('value') == res_def['value'])
    # change the job def and make sure the resource doesn't get registered again
    job_def['purpose'] = 'mayhem'
    consumer.task.add(job_def, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    # should only be one callback here
    assert(len(consumer.job_manager.resources['resource'][res_def['id']]) == 1)
    removed = consumer.task.remove(
        str(res_def['id']), type='resource', tenant=TENANT
    )  # explicit cast to string
    assert(removed is True)
    sleep(redis_subscribe_delay)
    # job stopped because of unmet depedency
    assert(_job.status is JobStatus.STOPPED)
    removed = consumer.task.remove(_id, type='job', tenant=TENANT)
    assert(removed is True)


@pytest.mark.integration
def test_consumer__job_registration_failed_missing_resource(consumer: BaseConsumer):
    redis_subscribe_delay = 0.05
    _id = '003'
    job_id = f'{TENANT}:{_id}'
    job_def = {
        'id': _id,
        'purpose': 'counter',
        'resources': 'res-002'
    }
    consumer.task.add(job_def, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    assert(job_id in consumer.job_manager.jobs.keys())
    _job: BaseJob = consumer.job_manager.jobs[job_id]
    # job stopped because of unmet depedency
    assert(_job.status is JobStatus.STOPPED)
    removed = consumer.task.remove(_id, type='job', tenant=TENANT)
    assert(removed is True)


@pytest.mark.integration
def test_consumer__job_registration_hanging_resource_reference(consumer: BaseConsumer):
    redis_subscribe_delay = 0.25
    _id = '004'
    job_id = f'{TENANT}:{_id}'
    res_def = {
        'id': f'res-{_id}',
        'value': 1000000
    }
    job_def = {
        'id': _id,
        'purpose': 'counter',
        'resources': f'res-{_id}'
    }
    # add job with missing resource
    consumer.task.add(job_def, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    _job: BaseJob = consumer.job_manager.jobs[job_id]
    # job starts dead
    assert(_job.status is JobStatus.STOPPED)
    # add missing resource
    consumer.task.add(res_def, type='resource', tenant=TENANT)
    sleep(redis_subscribe_delay)
    # job got resource
    assert(_job.resources['resource']['id'] == res_def['id'])
    # remove job, leave resource
    removed = consumer.task.remove(_id, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    assert(removed is True)
    # job is now gone
    assert(job_id not in consumer.job_manager.jobs.keys())
    res_def = {
        'id': f'res-{_id}',
        'value': 1000003
    }
    consumer.task.add(res_def, type='resource', tenant=TENANT)
    # this shouldn't raise an error on the now missing job
    sleep(redis_subscribe_delay)
    # add the job again
    consumer.task.add(job_def, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    _job = consumer.job_manager.jobs[job_id]
    assert(_job.status is JobStatus.NORMAL)
    # resource still correct with new version in resurrected job
    assert(_job.resources['resource']['id'] == res_def['id'])
    removed = consumer.task.remove(_id, type='job', tenant=TENANT)
    removed = consumer.task.remove(str(res_def['id']), type='resource', tenant=TENANT)


@pytest.mark.integration
def test_consumer__job_multicast_receive_resource(consumer: BaseConsumer):
    redis_subscribe_delay = 0.25
    # redis_subscribe_delay = 1  # lots of checking on job change
    _id = '005'
    res_def = {
        'id': f'res-{_id}',
        'value': 1000000
    }
    job_def1 = {
        'id': f'{_id}-1',
        'resources': f'res-{_id}'
    }
    job_def2 = {
        'id': f'{_id}-2',
        'resources': f'res-{_id}'
    }
    # add job with missing resource
    consumer.task.add(job_def1, type='job', tenant=TENANT)
    consumer.task.add(job_def2, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    job1: BaseJob = consumer.job_manager.jobs[f'{TENANT}:005-1']
    job2: BaseJob = consumer.job_manager.jobs[f'{TENANT}:005-2']
    # job starts dead
    assert(job1.status is JobStatus.STOPPED)
    assert(job2.status is JobStatus.STOPPED)
    # add missing resource
    consumer.task.add(res_def, type='resource', tenant=TENANT)
    sleep(redis_subscribe_delay)
    # job got resource
    assert(job1.status is JobStatus.NORMAL)
    assert(job2.status is JobStatus.NORMAL)
    # resource still correct with new version in resurrected job
    removed = consumer.task.remove(str(res_def['id']), type='resource', tenant=TENANT)
    sleep(redis_subscribe_delay)
    assert(job1.status is JobStatus.STOPPED)
    assert(job2.status is JobStatus.STOPPED)
    removed = consumer.task.remove('005-1', type='job', tenant=TENANT)
    removed = consumer.task.remove('005-2', type='job', tenant=TENANT)
    assert(removed is True)


@pytest.mark.integration
def test_consumer__job_persistence(consumer):
    redis_subscribe_delay = 0.25
    _id = '006'
    job_id = f'{TENANT}:{_id}'
    job_def = {'id': _id, 'purpose': 'counter'}
    consumer.task.add(job_def, type='job', tenant=TENANT)
    sleep(redis_subscribe_delay)
    assert(job_id in list(consumer.job_manager.jobs.keys()))
    consumer.job_manager.stop()
    consumer.job_manager._init_jobs()
    sleep(redis_subscribe_delay)
    assert(job_id in consumer.job_manager.jobs.keys())
    consumer.task.remove(_id, type='job', tenant=TENANT)


# mock consumer
@pytest.mark.unit
def test_load_schema_validate(mocked_consumer):
    c = mocked_consumer
    permissive = c.load_schema(os.path.join(here, 'assets/schema/permissive.json'))
    strict = c.load_schema(os.path.join(here, 'assets/schema/strict.json'))
    job = {'a': 1}
    assert(c.validate(job, schema=permissive) is True)
    assert(c.validate(job, schema=strict) is False)


######
#
#  TASK TESTS
#
#####

redis_messages = [
    {'id': '00001', 'a': 1},
    {'id': '00002', 'a': 2},
    {'id': '00022', 'a': 3}
]


@pytest.mark.integration
@pytest.mark.parametrize("name,args,expected", [
                        ('remove', ['00001', 'test'], False),
                        ('exists', ['00001', 'test'], False),
                        ('add', [{'id': '00001'}, 'test'], True),
                        ('exists', ['00001', 'test'], True),
                        ('remove', ['00001', 'test'], True),
                        ('exists', ['00001', 'test'], False)
])
def test_redis_io(name, args, expected, task_helper):
    fn = getattr(task_helper, name)
    _args = args[:]
    _args.append(TENANT)
    res = fn(*_args)
    assert(res == expected)


@pytest.mark.integration
def test_redis_get_methods(task_helper: TaskHelper):
    tasks = redis_messages
    _type = 'test'
    for t in tasks:
        assert(task_helper.add(t, _type, tenant=TENANT) is True)
        assert(task_helper.exists(str(t['id']), _type, tenant=TENANT) is True)
    for t in tasks:
        _id = str(t['id'])
        from_redis = task_helper.get(_id, _type, tenant=TENANT)
        assert(from_redis.get('id') == _id)
    try:
        task_helper.get('fake_id', _type, tenant=TENANT)
    except ValueError:
        pass
    else:
        assert(False)
    redis_ids = list(task_helper.list(_type, tenant=TENANT))  # {id}
    assert(all([t['id'] in redis_ids for t in tasks]))
    redis_ids = list(task_helper.list(tenant=TENANT))  # {tenant}:{id}
    LOG.debug(redis_ids)
    for t in tasks:
        assert(any([t['id'] in key for key in redis_ids]))


@pytest.mark.integration
@pytest.mark.parametrize("message,sub", [
    (redis_messages[0], {'pattern': '*_test:*'}),
    (redis_messages[1], {'pattern': '*_test:*:00002'})
])
def test_redis_subscibe__succeed(task_helper, message, sub):
    # TaskID in redis is : _{type}:{tenant}:{_id}
    redis_subscribe_delay = 0.05
    _type = 'test'
    LOG.debug(f'Msg ID is : {TENANT}:_{_type}:{message["id"]}')
    callable: MockCallable = MockCallable()
    task_helper.subscribe(callable.set_value, **sub)
    task_helper.add(message, _type, tenant=TENANT)
    assert(task_helper.exists(message['id'], _type, tenant=TENANT))
    sleep(.25)
    # listen for set and check value
    assert(message['a'] == callable.value.data['a'])
    # delete the message
    task_helper.remove(message['id'], _type, tenant=TENANT)
    assert(task_helper.exists(message['id'], _type, tenant=TENANT) is False)
    sleep(redis_subscribe_delay)
    # get delete message
    LOG.debug(callable.value)
    assert(callable.value.data is None and callable.value.type == 'del')


@pytest.mark.integration
@pytest.mark.parametrize("message,sub", [
    (redis_messages[1], {'pattern': '*_test:*:00002_01'})
])
def test_redis_subscibe__fail(task_helper, message, sub):
    redis_subscribe_delay = 0.05
    _type = 'test'
    LOG.debug(f'Msg ID is : _{_type}:{message["id"]}')
    callable: MockCallable = MockCallable()
    task_helper.subscribe(callable.set_value, **sub)
    task_helper.add(message, _type, tenant=TENANT)
    assert(task_helper.exists(message['id'], _type, tenant=TENANT))
    sleep(redis_subscribe_delay)
    assert(callable.value is None)


@pytest.mark.integration
def test_redis_subscibe_multiple__succeed(task_helper: TaskHelper):
    suite = [
        ({'id': '00022', 'a': 3}, {'pattern': '*_test:*:000*2'}, (3, None, 3), (3, 2, 3)),
        ({'id': '00002', 'a': 2}, {'pattern': '*_test:*:00002'}, (2, 2, 2), (2, 2, 2)),
        ({'id': '00001', 'a': 1}, {'pattern': '*_test:*'}, (2, 2, 1), (2, 2, 1)),
    ]
    callables: List[MockCallable] = [MockCallable() for i in range(len(suite))]
    _type = 'test'

    # subscribe the listeners
    for x, (message, sub, fwd, rev) in enumerate(suite):
        task_helper.subscribe(callables[x].set_value, **sub)

    # send messages in order
    for (message, sub, fwd, rev) in suite:
        task_helper.add(message, _type, tenant=TENANT)
        LOG.debug(f'ADD {message["id"]} : {message["a"]}')
        assert(task_helper.exists(str(message['id']), _type, tenant=TENANT))
        sleep(.25)
        res = tuple([c.value.data.get('a')
                    if (c.value and c.value.data)  # required for type checker
                    else None
                    for c in callables])
        LOG.debug(json.dumps([c.value.data if c.value else None for c in callables], indent=2))
        assert(res == fwd)

    # send the messages in reverse order
    for (message, sub, fwd, rev) in suite[::-1]:
        task_helper.add(message, _type, tenant=TENANT)
        LOG.debug(f'ADD {message["id"]} : {message["a"]}')
        assert(task_helper.exists(str(message['id']), _type, tenant=TENANT))
        sleep(.25)
        res = tuple([c.value.data.get('a')
                    if (c.value and c.value.data)  # required for type checker
                    else None
                    for c in callables])
        LOG.debug(json.dumps([c.value.data if c.value else None for c in callables], indent=2))
        assert(res == rev)
