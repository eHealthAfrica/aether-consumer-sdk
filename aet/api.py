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

import logging
from functools import wraps, partialmethod
import json

from flask import Flask, Response, request, jsonify
from webtest.http import StopableWSGIServer

from .logger import LOG


class APIServer(object):

    _allowed_types = ['job']  # , 'resource']

    def __init__(self, consumer, task_manager, settings):
        self.settings = settings
        self.consumer = consumer
        self.task = task_manager

    def serve(self):
        name = self.settings.get('CONSUMER_NAME')
        LOG.info(f'Starting API: {name}')
        self.app = Flask(name)  # noqa
        try:
            handler = self.app.logger.handlers[0]
        except IndexError:
            handler = logging.StreamHandler()
        finally:
            handler.setFormatter(logging.Formatter(
                '%(asctime)s [ConsumerAPI] %(levelname)-8s %(message)s'))
            self.app.logger.addHandler(handler)
            log_level = logging.getLevelName(self.settings
                                             .get('log_level', 'DEBUG'))
            self.app.logger.setLevel(log_level)

        self.app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

        server_ip = self.settings.get('server_ip', '0.0.0.0')
        server_port = int(self.settings.get('EXPOSE_PORT', 9013))
        self.admin_name = self.settings.get('ADMIN_USER', 'admin')
        self.admin_password = self.settings.get('ADMIN_PW', 'password')
        self.http = StopableWSGIServer.create(
            self.app.wsgi_app,
            port=server_port,
            host=server_ip
        )
        self.app.logger.debug('Http Serve start.')
        self.add_endpoints()
        self.app.logger.debug('Http Live on {server_ip} : {server_port}'.format(
            server_ip=server_ip,
            server_port=server_port
        ))

    def stop(self, *args, **kwargs):
        self.app.logger.info('Stopping API')
        self.http.shutdown()

    # Flask Functions

    # def add_endpoints(self):
    #     # URLS configured here
    #     # Add endpoints for all registered types
    #     for _type in type(self)._allowed_types:
    #         self.register(
    #             f'{_type}/add',
    #             partialmethod(add, _type),
    #             methods=['POST'])
    #         self.register(
    #             f'{_type}/delete',
    #             partialmethod(remove, _type))
    #         self.register(
    #             f'{_type}/update',
    #             partialmethod(add, _type),
    #             methods=['POST'])
    #         self.register(
    #             f'{_type}/validate',
    #             partialmethod(validate, _type))
    #         self.register(
    #             f'{_type}/get',
    #             partialmethod(get, _type))
    #         self.register(
    #             f'{_type}/list',
    #             partialmethod(_list, _type))
    #     self.register('healthcheck', self.request_healthcheck)

    # def register(self, route_name, fn, **options):
    #     self.app.add_url_rule('/%s' % route_name, route_name, view_func=fn, **options)

    # Basic Auth implementation

    def check_auth(self, username, password):
        return username == self.admin_name and password == self.admin_password

    def request_authentication(self):
        return Response('Bad Credentials', 401,
                        {'WWW-Authenticate': 'Basic realm="Login Required"'})

    def requires_auth(f):
        @wraps(f)
        def decorated(self, *args, **kwargs):
            auth = request.authorization
            if not auth or not self.check_auth(auth.username, auth.password):
                return self.request_authentication()
            return f(self, *args, **kwargs)
        return decorated

    # Exposed endpoints

    def request_healthcheck(self):
        with self.app.app_context():
            return Response({"healthy": True})

    @requires_auth
    def add(self, _type):
        return self.handle_crud(request, 'CREATE', _type)

    @requires_auth
    def remove(self, _type):
        return self.handle_crud(request, 'DELETE', _type)

    @requires_auth
    def get(self, _type):
        return self.handle_crud(request, 'READ', _type)

    @requires_auth
    def _list(self, _type):
        with self.app.app_context():
            return jsonify(dict(self.consumer.list(_type=_type)))

    @requires_auth
    def validate(self, _type):
        res = self.consumer.validate(request.get_json(), _type)
        with self.app.app_context():
            return jsonify({'valid': res})

    def handle_crud(self, request, operation, _type):
        self.app.logger.debug(request)
        _id = request.args.get('id', None)
        if operation == 'CREATE':
            if self.consumer.validate_job(request.get_json()):
                response = self.task.add(request.get_json(), type='job')
            else:
                response = False
        if operation == 'DELETE':
            response = self.task.remove(_id, type='job')
        if operation == 'READ':
            response = json.loads(self.task.get(_id, type='job'))
        with self.app.app_context():
            return jsonify(response)
