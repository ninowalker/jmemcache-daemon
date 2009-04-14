#!/bin/sh
LOG=/var/log/jmemcached/out.log

if [ -z "$JMEMCACHED_HOME" -o ! -d "$JMEMCACHED_HOME" ] ; then
  if [ -d /usr/share/jmemcached ] ; then
     JMEMCACHED_HOME=/usr/share/jmemcached
  fi
fi

java -jar $JMEMCACHED_HOME/${project.artifactId}-${project.version}-main.jar  >>$LOG 2>&1
 

