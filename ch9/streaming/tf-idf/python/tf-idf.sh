/usr/bin/hadoop  jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
-D mapreduce.reduce.tasks=0 \
-input /tmp/df-out.tsv/part-00000 \
-output /tmp/tf-idf.out \
-file tf-idf.py \
-mapper "python tf-idf.py 15578" \
