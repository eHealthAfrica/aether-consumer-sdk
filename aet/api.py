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
from functools import wraps

from flask import Flask, Response, request, jsonify
from webtest.http import StopableWSGIServer

from .logger import LOG


class APIServer(object):

    def __init__(self, consumer, settings):
        self.settings = settings
        self.consumer = consumer

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

    def add_endpoints(self):
        # URLS configured here
        self.register('jobs/add', self.add_job, methods=['POST'])
        self.register('jobs/delete', self.remove_job)
        self.register('jobs/update', self.add_job, methods=['POST'])
        self.register('jobs/validate', self.validate_job)
        self.register('jobs/get', self.get_job)
        self.register('jobs/list', self.list_jobs)
        self.register('healthcheck', self.request_healthcheck)

    def register(self, route_name, fn, **options):
        self.app.add_url_rule('/%s' % route_name, route_name, view_func=fn, **options)

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
    def add_job(self):
        return self.handle_job_crud(request, 'CREATE')

    @requires_auth
    def remove_job(self):
        return self.handle_job_crud(request, 'DELETE')

    @requires_auth
    def get_job(self):
        return self.handle_job_crud(request, 'READ')

    @requires_auth
    def list_jobs(self):
        with self.app.app_context():
            return jsonify(dict(self.consumer.list_jobs()))

    @requires_auth
    def validate_job(self):
        res = self.consumer.validate_job(**request.data)
        with self.app.app_context():
            return jsonify({'valid': res})

    def handle_job_crud(self, request, _type):
        self.app.logger.debug(request)
        _id = request.args.get('id', None)
        if _type == 'CREATE':
            response = jsonify(self.consumer.add_job(job=request.get_json()))
        if _type == 'DELETE':
            response = jsonify(self.consumer.remove_job(_id))
        if _type == 'READ':
            response = jsonify(self.consumer.get_job(_id))
        with self.app.app_context():
            return response
