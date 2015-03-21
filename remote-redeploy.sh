#!/bin/bash
sbt "set offline := true" publishLocal pack
scp target/pack/lib/gearpump-experiments-yarn_2.11-0.3.0-rc2-SNAPSHOT.jar 192.168.1.158:/home/pancho/git/gearpump/target/pack/lib/
ssh 192.168.1.158 "cd /home/pancho/git/gearpump/ && ./redeploy.sh"
