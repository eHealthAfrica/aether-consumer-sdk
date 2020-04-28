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
import json
from typing import ClassVar, Dict, List, TYPE_CHECKING, Union

from flask import Flask, Request, Response, request, jsonify
from webtest.http import StopableWSGIServer

from .exceptions import ConsumerHttpException
from .logger import get_logger
from .settings import CONSUMER_CONFIG

LOG = get_logger('API')

if TYPE_CHECKING:  # pragma: no cover
    from .consumer import BaseConsumer
    from aether.python.redis.task import TaskHelper
    from .settings import Settings


DEFAULT_TENANT = CONSUMER_CONFIG.get('DEFAULT_TENANT', 'no-tenant')


class APIServer(object):

    # consumed by the restrict_types decorator
    _allowed_types: ClassVar[Dict[str, List]]

    def __init__(
        # type declaration of the arguments in the usual way causes
        # circular imports, so we do it in the init method instead.
        self,
        consumer: 'BaseConsumer',
        task_manager: 'TaskHelper',
        settings: 'Settings'
    ) -> None:
        type(self)._allowed_types = {
            _cls.name: _cls.public_actions for _cls in consumer.job_class._resources
        }
        type(self)._allowed_types['job'] = consumer.job_class.public_actions
        LOG.debug(f'Allowed Operations: {type(self)._allowed_types}')
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

    #######
    #
    # Flask Functions
    #
    #######

    # Restrict types that can be input into API
    #   Also restricts which methods are allowed in a type
    #   Set in Class._allowed_types

    def restrict_types(operation=None):  # TODO # Can't get this typed properly
        def decorator(f):
            @wraps(f)
            def decorated(self, *args, **kwargs):
                _type = kwargs.get('_type')
                if operation is not None:
                    # comes from decorator
                    kwargs['operation'] = operation
                    op = operation
                elif 'operation' in kwargs:
                    # comes from request
                    op = kwargs['operation']
                else:
                    return Response('Operation not properly set', 502)
                type_definitions = type(self)._allowed_types
                if _type not in type_definitions:
                    return Response('Not Found', 404)
                elif op not in type_definitions[_type]:
                    return Response(f'{op} not valid for {_type}', 405)
                return f(self, *args, **kwargs)
            return decorated
        return decorator

    def add_endpoints(self) -> None:
        # URLS configured here
        # this MUST be done at runtime. Can't set the routes via decorator
        # Add endpoints for all registered types
        self.register(
            '<string:_type>/add',
            self.add,
            methods=['POST'])

        self.register(
            '<string:_type>/delete',
            self.remove,
            methods=['GET', 'POST', 'DELETE'])

        self.register(
            '<string:_type>/update',
            self.add,
            methods=['POST'])

        self.register(
            '<string:_type>/validate',
            self.validate,
            methods=['POST'])

        self.register(
            '<string:_type>/get',
            self.get,
            methods=['GET', 'POST'])

        self.register(
            '<string:_type>/list',
            self._list)

        self.register(
            '<string:_type>/<string:operation>',
            self.handle_other,
            methods=['GET', 'POST'])

        self.register('health', self.request_healthcheck)

    def register(self, route_name, fn, **options) -> None:
        self.app.add_url_rule('/%s' % route_name, route_name, view_func=fn, **options)

    #######
    #
    # Auth / Control
    #
    #######

    # Basic Auth implementation

    def check_auth(self, username, password) -> bool:
        return username == self.admin_name and password == self.admin_password

    def request_authentication(self) -> Response:
        return Response('Bad Credentials', 401,
                        {'WWW-Authenticate': 'Basic realm="Login Required"'})

    # auth enabled on routes via decorator inclusion
    def requires_auth(f):  # TODO # Can't get this typed properly
        @wraps(f)
        def decorated(self, *args, **kwargs):
            # if we're running tenanted then we don't use basic auth
            if not self.settings.get('TENANCY_HEADER'):
                auth = request.authorization
                if not auth or not self.check_auth(auth.username, auth.password):
                    return self.request_authentication()
            return f(self, *args, **kwargs)
        return decorated

    def check_tenant(f):
        @wraps(f)
        def decorated(self, *args, **kwargs):
            tenancy_header = self.settings.get('TENANCY_HEADER')
            if not tenancy_header:
                return f(self, *args, **kwargs)
            tenant = request.headers.get(tenancy_header)
            if not tenant:
                return Response(f'Missing tenant header: {tenancy_header}', 400)
            kwargs['tenant'] = tenant
            return f(self, *args, **kwargs)
        return decorated

    #######
    #
    # Exposed endpoints
    #
    #######

    def request_healthcheck(self) -> Response:
        with self.app.app_context():
            return Response({"healthy": True})

    # Generic CRUD

    @restrict_types('CREATE')
    @requires_auth
    @check_tenant
    def add(self, tenant=None, _type=None, operation=None):
        return self.handle_crud(request, operation, _type, tenant)

    @restrict_types('DELETE')
    @requires_auth
    @check_tenant
    def remove(self, tenant=None, _type=None, operation=None):
        return self.handle_crud(request, operation, _type, tenant)

    @restrict_types('READ')
    @requires_auth
    @check_tenant
    def get(self, tenant=None, _type=None, operation=None):
        return self.handle_crud(request, operation, _type, tenant)

    # List of Assets of _type

    @restrict_types('LIST')
    @requires_auth
    @check_tenant
    def _list(self, tenant=None, _type=None, operation=None):
        return self.handle_crud(request, operation, _type, tenant)

    # Validation of asset of _type

    @restrict_types('VALIDATE')
    @requires_auth
    @check_tenant
    def validate(self, tenant=None, _type=None, operation=None):
        res = self.consumer.validate(request.get_json(), _type)
        with self.app.app_context():
            return jsonify({'valid': res})

    @restrict_types()
    @requires_auth
    @check_tenant
    def handle_other(self, tenant=None, _type=None, operation=None):
        _cls = self.consumer._classes.get(_type)
        if not _cls:
            return Response(f'Invalid type "{_type}".', 400)
        if operation not in _cls.public_actions:
            return Response(f'"{_type}" does not allow operation {operation}', 400)
        try:
            res = self.consumer.dispatch(tenant, _type, operation, request)
        except ConsumerHttpException as che:
            return che.as_response()
        if isinstance(res, Response):
            return res
        with self.app.app_context():
            return jsonify(res)

    #######
    #
    # CRUD Handling
    #
    #######

    def handle_crud(self, request: Request, operation: str, _type: str, tenant: str):
        self.app.logger.debug(f'tenant: {tenant} request: {request}')
        _id = request.values.get('id', None)
        response: Union[str, List, Dict, bool]  # anything compatible with jsonify
        if operation == 'CREATE':
            if self.consumer.validate(request.get_json(), _type, tenant):
                response = self.task.add(request.get_json(), _type, tenant)
            else:
                response = False
        if operation == 'DELETE':
            if not _id:
                return Response('Argument "id" is required', 400)
            try:
                response = self.task.remove(_id, _type, tenant)
            except ValueError:
                return ConsumerHttpException(
                    f'{_type} object with id : {_id} not found', 404).as_response()
        if operation == 'READ':
            if not _id:
                return Response('Argument "id" is required', 400)
            try:
                response = json.loads(
                    json.dumps(self.task.get(_id, _type, tenant))
                )
                try:
                    response = self.consumer.dispatch(tenant, _type, 'mask_config', response)
                except ConsumerHttpException as che:
                    return che.as_response()
            except ValueError:
                return ConsumerHttpException(
                    f'{_type} object with id : {_id} not found', 404).as_response()
        if operation == 'LIST':
            response = list(self.task.list(_type, tenant))
        with self.app.app_context():
            return jsonify(response)
