#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

binDir=`dirname "$0"`
echo $binDir

HADOOP_HOME_PATH=/usr/hdp/current/hadoop-client
HADOOP_CONFIG_SCRIPT=$HADOOP_HOME_PATH/libexec/hadoop-config.sh
HADOOP_CLIENT_LIBS=$HADOOP_HOME_PATH/client
if [ -e $HADOOP_CONFIG_SCRIPT ] ; then
        .  $HADOOP_CONFIG_SCRIPT
else
        echo "Hadoop Client not Installed on Node"
        exit 1
fi

JRECORDJAR=`ls -1 $binDir/lib/JRecord*.jar`
JSONJAR=`ls -1 $binDir/lib/json*.jar`

export HADOOP_CLASSPATH=$JRECORDJAR:$JSONJAR
COPYBOOKJAR=`ls -1 $binDir/lib/copybook_formatter*.jar`

hadoop fs -mkdir -p /apps/copybook_formatter
hadoop fs -copyFromLocal -f $JRECORDJAR /apps/copybook_formatter/JRecordV2.jar
hadoop fs -copyFromLocal -f $COPYBOOKJAR /apps/copybook_formatter/copybook_formatter.jar
hadoop fs -copyFromLocal -f $JSONJAR /apps/copybook_formatter/json.jar