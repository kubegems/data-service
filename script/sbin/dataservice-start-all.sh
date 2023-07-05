#!/bin/bash
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

# description:  Start all Server
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

# Start all dataservice applications
info="We will start all dataservice applications, it will take some time, please wait"
echo ${info}


# set DATASERVICE_CONF_DIR
if [ "$DATASERVICE_CONF_DIR" = "" ]; then
  export DATASERVICE_CONF_DIR=$DATASERVICE_HOME/conf
fi

function isSuccess(){
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed${NC} to $1"
    echo ""
    exit 1
else
    echo -e "${GREEN}Succeed${NC} to $1"
    echo ""
fi
}

function startApp(){
echo "<-------------------------------->"
echo "Begin to start $SERVER_NAME"
SERVER_START_CMD="sh $DATASERVICE_HOME/sbin/dataservice-daemon.sh restart $SERVER_NAME"
$SERVER_START_CMD

isSuccess "start $SERVER_NAME"
echo "<-------------------------------->"
sleep 3
}


#gateway
export SERVER_NAME="gateway"
startApp


#quoto-config
SERVER_NAME="quoto-config"
startApp

#quoto-chatbot
SERVER_NAME="quoto-chatbot"
startApp

#quoto-roc
SERVER_NAME="quoto-roc"
startApp

#quoto-search
SERVER_NAME="quoto-search"
startApp

echo "start-all shell script executed completely"
sleep 15
echo "Start to check all dataserver microservice"

function checkServer() {
echo "<-------------------------------->"
echo "Begin to check $SERVER_NAME"
SERVER_CHECK_CMD="sh $DATASERVICE_HOME/sbin/dataservice-daemon.sh status $SERVER_NAME"
$SERVER_CHECK_CMD

if [ $? -ne 0 ]; then
      ALL_SERVER_NAME=$SERVER_NAME
      LOG_PATH=$DATASERVICE_HOME/logs/$ALL_SERVER_NAME.log
      echo "ERROR: your $ALL_SERVER_NAME microservice does not start successful !!! ERROR logs as follows :"
      echo "Please check detail log, log path :$LOG_PATH"
      echo '<---------------------------------------------------->'
      $ALL_SERVER_NAME "tail -n 50 $LOG_PATH"
      echo '<---------------------------------------------------->'
      echo "Please check detail log, log path :$LOG_PATH"
      exit 1
fi
echo "<-------------------------------->"
sleep 3
}

#gateway
export SERVER_NAME="gateway"
checkServer


#quoto-config
SERVER_NAME="quoto-config"
checkServer

#quoto-chatbot
SERVER_NAME="quoto-chatbot"
checkServer

#quoto-roc
SERVER_NAME="quoto-roc"
checkServer


#quoto-search
SERVER_NAME="quoto-search"
checkServer

echo "dataservice started successfully"
