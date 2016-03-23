#! /bin/bash

# Atom run server
cd /tmp/Atom-Hbase-Integration/lib
runningJar=$(ls Atom-Hbase-Integration*.jar)

echo " -- launching Atom CLIENT (NGINX) : nginx"
nginx

echo " -- launching Atom SERVER : java -jar ${runningJar} Atom-Hbase-Integration"
java -jar ${runningJar} Atom-Hbase-Integration
