#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# description:  Stop all Server
#
# Modified for Linkis 1.0.0
#Actively load user env
source /etc/profile
source ~/.bash_profile

cd `dirname $0`
cd ..
INSTALL_HOME=`pwd`

# set DATASERVICE_HOME
if [ "$DATASERVICE_HOME" = "" ]; then
  export DATASERVICE_HOME=$INSTALL_HOME
fi

info="We will stop all dataservice applications, it will take some time, please wait"
echo ${info}

# set DATASERVICE_CONF_DIR
if [ "$DATASERVICE_CONF_DIR" = "" ]; then
  export DATASERVICE_CONF_DIR=$DATASERVICE_HOME/conf
fi

function stopApp(){
echo "<-------------------------------->"
echo "Begin to stop $SERVER_NAME"
SERVER_STOP_CMD="sh $DATASERVICE_HOME/sbin/dataservice-daemon.sh stop $SERVER_NAME"
$SERVER_STOP_CMD

echo "<-------------------------------->"
}

#gateway
SERVER_NAME="gateway"
stopApp

#quoto-config
SERVER_NAME="quoto-config"
stopApp

#quoto-chatbot
SERVER_NAME="quoto-chatbot"
stopApp

#quoto-roc
SERVER_NAME="quoto-roc"
stopApp

#quoto-search
SERVER_NAME="quoto-search"
stopApp

echo "stop-all shell script executed completely"
