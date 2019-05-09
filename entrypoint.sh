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
    test_unit           : run unit tests with coverage output
    test_integration    : run integration tests

    """
}

function test_flake8 {
    flake8
}

function test_integration {
    echo 'Running Integration Tests...'
    pytest -m integration $PYTEST_OPT
}

function test_unit {
    echo 'Running Unit Tests...'
    pytest -m unit $PYTEST_OPT
}

export PYTHONDONTWRITEBYTECODE=1
PYTEST_OPT="-p no:cacheprovider"  # disable __pycache__ which pollutes local FS

case "$1" in

    test )
        test_flake8
        test_unit
        test_integration
    ;;

    test_lint )
        test_flake8
    ;;

    test_unit )
        test_unit
    ;;

    test_integration )
        test_integration
    ;;

    *)
        show_help
    ;;
esac
