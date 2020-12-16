#!/usr/bin/env bash
#
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
#
set -Eeuo pipefail


function show_help {
    echo """
    Commands
    ----------------------------------------------------------------------------

    test                : run all tests
    test_lint           : run flake8 tests
    test_unit           : run unit tests
    test_integration    : run integration tests

    """
}

export PYTHONDONTWRITEBYTECODE=1
PYTEST="pytest --cov-report term-missing --cov=aet -p no:cacheprovider"

case "$1" in

    serve )
        python ./serve.py "${@:2}"
    ;;

    test )
        echo 'Running ALL Tests...'
        flake8
        $PYTEST
    ;;

    test_lint )
        echo 'Running Lint Tests...'
        flake8
    ;;

    test_unit )
        echo 'Running Unit Tests...'
        flake8
        $PYTEST -m unit
    ;;

    test_ui )
        echo 'Running UI Tests...'
        flake8
        $PYTEST -m ui
    ;;

    test_integration )
        echo 'Running Integration Tests...'
        $PYTEST -m integration
    ;;

    *)
        show_help
    ;;
esac
