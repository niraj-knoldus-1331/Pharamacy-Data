#!/usr/bin/env bash

set -e
set -o pipefail
set -u
mvn clean install -DskipTests

echo "#######Copying Jar"
cp target/Pharamacy-Data.jar /jar/
ls target/