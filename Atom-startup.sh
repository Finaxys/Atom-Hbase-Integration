#! /bin/bash

# Atom run server
cd /public/atom/atom/1.0
runningJar=$(ls atom-*.jar)

echo " -- launching Atom CLIENT (NGINX) : nginx"
nginx

echo " -- launching Atom SERVER : java -jar ${runningJar} Atom-Hbase-Integration"
java -jar ${runningJar} org.AHI.Application
