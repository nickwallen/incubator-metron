#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
METRON_VERSION=${project.version}
METRON_HOME=/usr/metron/$METRON_VERSION
#TODO how to correctly set SPARK_HOME?
SPARK_HOME=/usr/hdp/current/spark
SPARK_PROFILER_JAR=${project.artifactId}-$METRON_VERSION.jar

# spark-submit settings
APP_NAME="Apache Metron Batch Profiler"
MAIN_CLASS=org.apache.metron.profiler.spark.BatchProfiler
MASTER_URL=local
DEPLOY_MODE=client

# yarn settings
#TODO how to handle YARN-specific settings like --queue QUEUE
DRIVER_MEM=1G
EXECUTOR_MEM=1G
EXECUTOR_CORES=1
DRIVER_CORES=1

# launch the batch profiler
$SPARK_HOME/bin/spark-submit \
  --name $APP_NAME \
  --class $MAIN_CLASS \
  --master $MASTER_URL \
  --deploy-mode $DEPLOY_MODE \
#  --driver-memory $DRIVER_MEM \
#  --executor-memory $EXECUTOR_MEM \
#  --executor-cores $EXECUTOR_CORES \
#  --driver-cores $DRIVER_CORES \
  $METRON_HOME/lib/$SPARK_PROFILER_JAR