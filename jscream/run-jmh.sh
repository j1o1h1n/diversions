#!/bin/zsh
set -euo pipefail

cd "$(dirname "$0")"

mvn -q test-compile dependency:build-classpath \
  -DincludeScope=test \
  -Dmdep.outputAbsoluteArtifacts=true \
  -Dmdep.outputFile=target/jmh.classpath

CLASSPATH="target/test-classes:target/classes:$(cat target/jmh.classpath)"

exec java -cp "$CLASSPATH" org.openjdk.jmh.Main "$@"
