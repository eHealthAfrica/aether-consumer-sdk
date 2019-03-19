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


# Define help message
show_help() {
    echo """
    Commands
    ----------------------------------------------------------------------------
    bash                : run bash
    build               : build python wheel of library in /dist
    eval                : eval shell command

    pip_freeze          : freeze pip dependencies and write to requirements.txt

    test                : run tests
    test_lint           : run flake8 tests
    test_unit           : run tests with coverage output
    test_integration    : run tests with coverage output

    """
}

test_flake8() {
    flake8 /code/. --config=/code/conf/extras/flake8.cfg
}

test_integration() {
    echo 'Running Integration Tests...'
    export PYTHONDONTWRITEBYTECODE=1 
    pytest --cov-report term-missing --cov=aet tests/ -m integration -p no:cacheprovider  # disable __pycache__ which pollutes local FS
    cat /code/conf/extras/good_job.txt
}

test_unit() {
    echo 'Running Unit Tests...'
    export PYTHONDONTWRITEBYTECODE=1 
    pytest --cov-report term-missing --cov=aet tests/ -m unit -p no:cacheprovider  # disable __pycache__ which pollutes local FS
    cat /code/conf/extras/good_job.txt
}


test_coverage() {
    echo 'Running All Tests...'
    export PYTHONDONTWRITEBYTECODE=1 
    pytest --cov-report term-missing --cov=aet tests/ -p no:cacheprovider
    cat /code/conf/extras/good_job.txt
}

case "$1" in
    bash )
        bash
    ;;

    eval )
        eval "${@:2}"
    ;;

    manage )
        ./manage.py "${@:2}"
    ;;

    pip_freeze )

        rm -rf /tmp/env
        pip3 install -f ./conf/pip/dependencies -r ./conf/pip/primary-requirements.txt --upgrade

        cat /code/conf/pip/requirements_header.txt | tee conf/pip/requirements.txt
        pip3 freeze --local | grep -v appdir | tee -a conf/pip/requirements.txt
    ;;

    test)
        test_flake8
        test_coverage
    ;;

    test_unit)
        test_flake8
        test_unit

    ;;

    test_integration)
        test_flake8
        test_integration
    ;;

    test_lint)
        test_flake8
    ;;

    build)
        # remove previous build if needed
        rm -rf dist
        rm -rf build
        rm -rf .eggs
        rm -rf aet.consumer.egg-info

        # create the distribution
        python setup.py bdist_wheel --universal

        # remove useless content
        rm -rf build
        rm -rf aet.consumer.egg-info
    ;;

    help)
        show_help
    ;;

    *)
        show_help
    ;;
esac
