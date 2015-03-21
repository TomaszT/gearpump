#!/bin/bash
#sbt "set offline := true" publishLocal pack
#scp target/pack/lib/gearpump-experiments-yarn_2.11-0.3.0-rc2-SNAPSHOT.jar 192.168.1.158:/home/pancho/git/gearpump/target/pack/lib/
sbt publishLocal pack
~/hadoop/bin/yarn application -list 2>&1 | grep application_ | cut -f1 | xargs -n1 -t ~/hadoop/bin/yarn application -kill
~/hadoop/bin/hdfs dfs -put -f target/pack/lib/gearpump-experiments-yarn_2.11-0.3.0-rc2-SNAPSHOT.jar /user/gearpump/jars/
ssh "192.168.1.158" /home/pancho/git/gearpump/target/pack/bin/yarnclient
