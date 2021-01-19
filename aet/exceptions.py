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

from flask import Response


class ConsumerHttpException(Exception):
    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        if isinstance(message, BaseException):
            self.message = str(message)
        else:
            self.message = message
        self.status_code = status_code

    def as_response(self):
        return Response(self.message, self.status_code)


class MessageHandlingException(Exception):
    '''
    Generic exception thrown by implementors of BaseJob within their
    implementation of _get_messages or _handle_messages which will then trigger
    the _on_message_handle_exception() block
    '''

    def __init__(self, message: str, details=None, **kwargs):
        super().__init__(message)
        self.details = details or {}
