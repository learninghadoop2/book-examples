#!/bin/env/bash
/usr/bin/hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input /tmp/tweets.json \
    -output /tmp/tweets.cnt \
    -mapper /bin/cat \
    -reducer /usr/bin/wc
