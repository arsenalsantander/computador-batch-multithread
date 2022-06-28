#!/bin/bash
ROOT_BASE=/c/computador/computador-batch
JAVA_HOME=$ROOT_BASE/java/jre-11
JAVA=$JAVA_HOME/bin/java
 
#sigla
SIGLA=xxx
#component
COMPONENT_NAME=demo-computador-batch-multithread
RUN_COMPONENT=$COMPONENT_NAME.jar
#path for component
ROOT=$ROOT_BASE/bin/$SIGLA/$COMPONENT_NAME
 
#input
INPUT=$ROOT_BASE/file/$SIGLA/$COMPONENT_NAME/input
#output
OUTPUT=$ROOT_BASE/file/$SIGLA/$COMPONENT_NAME/output	
  
#log component
BASE_LOG_PATH=$ROOT_BASE/bin/$SIGLA/$COMPONENT_NAME/log
 
$JAVA -jar $RUN_COMPONENT --spring.profiles.active=dev