#!/bin/sh

schematool -info -dbType derby
if [ $? -ne 0 ]; then 
  schematool -initSchema -dbType derby
fi

/opt/hive/bin/hive --service metastore
