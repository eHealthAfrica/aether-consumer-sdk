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
set -Eeuo pipefail

function kill_all {
  echo "_________________________________________________ Killing Containers"
  docker-compose kill
  docker-compose down
}

MODE=test
SCAFFOLD=True

if [[ "${1:-}" == "integration" ]]; then
    MODE=test_integration
    SCAFFOLD=True

elif [[ "${1:-}" == "unit" ]]; then
    MODE=test_unit
    SCAFFOLD=False

elif [[ "${1:-}" == "lint" ]]; then
    MODE=test_lint
    SCAFFOLD=False
fi

kill_all
if [[ $SCAFFOLD == True ]]
then
    echo "_________________________________________________ Starting Kafka"
    docker-compose up -d  zookeeper-test kafka-test
fi

docker-compose up -d redis-test

echo "_________________________________________________ Starting Python Tests in mode $MODE"

echo "_________________________________________________ Building container"
docker-compose build consumer-test

echo "_________________________________________________ Running tests: $MODE"
docker-compose run consumer-test $MODE

echo "_________________________________________________ Finished Test"
kill_all

echo "_________________________________________________ END"
