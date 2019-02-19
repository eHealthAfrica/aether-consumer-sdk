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

function kill_all(){
  echo "_________________________________________________ Killing Containers"
  $DC_TEST kill
  $DC_TEST down
}

DC_TEST="docker-compose -f docker-compose.yml"
MODE=null
if [[ $1 == "integration" || $1 == "all" ]]
then
    MODE="test_integration"
    scaffold=True
elif [[ $1 == "unit" ]]
then
    MODE="test_unit"
    scaffold=False
else
    echo "no valid (unit, integration, all) test type indicated. Running 'all'."
    MODE="test"
    scaffold=True
fi

kill_all
if [[ $scaffold == True ]]
then
    echo "_____________________________________________ Starting Kafka"
    $DC_TEST up -d zookeeper-test kafka-test
fi

echo "_____________________________________________ Starting Python Tests in mode $MODE"

echo "_________________________________________________ Building container"
$DC_TEST build consumer-test
echo "_________________________________________________ Running tests: $MODE"
$DC_TEST run consumer-test $MODE

echo "_____________________________________________ Finished Test"

kill_all

echo "_____________________________________________ END"
