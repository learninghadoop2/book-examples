TF-IDF
------

Definition:

tf = # of times term appears in a document (raw frequency)
df = # of documents with term  in it
idf = log(#  of documents / # documents with term in it)

tf-idf = tf * idf

We break down tf-idf calculation into three jobs, excecuted in the following order

1. The first one calculates tf: map-tf.py and reduce-tf.py:
2. The second one calculates idf: map-df.py and reduce-df.py
3. finally we calculate per tweet tf-idf: tf-idf.py

An helper mr job is provided to count the total number of tweets in the dataset (map-num_doc.py and reduce-num_doc.py).

Command line
============
Make sure that the number of tweets in the dataset is a known quantity.

$ cat tweets.json | python map-num_doc.py | python reduce-num_doc.py
15578

1. Calculate TF
cat /tmp/tweets.json  |  python map-tf.py  | sort -k1,2  | python reduce-tf.py > /tmp/tf-out.tsv

2. Calculate IDF
cat /tmp/tf-out.tsv  |  python map-df.py | python reduce-df.py > /tmp/df-out.tsv

3. Calculate TF-IDF
cat /tmp/df-out.tsv | python tf-idf.py 15578


Hadoop Streaming
================

0. Calculate the number of tweets

/usr/bin/hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input tweets.json \
    -output tweets.cnt \
    -mapper /bin/cat \
    -reducer /usr/bin/wc

1. Calculate TF

Note that we specify which fields belong to the key (for shuffling) in the comparator.

/usr/bin/hadoop  jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
-D map.output.key.field.separator=\t \
-D stream.num.map.output.key.fields=2 \
-D mapreduce.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.KeyFieldBasedComparator \
-D mapreduce.text.key.comparator.options=-k1,2 \
-input tweets.json \
-output /tmp/tf-out.tsv \
-file map-tf.py \
-mapper "python map-tf.py" \
-file reduce-tf.py \
-reducer "python reduce-tf.py"

2. Calculate DF

/usr/bin/hadoop  jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
-D map.output.key.field.separator=\t \
-D stream.num.map.output.key.fields=1 \
-D mapreduce.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.KeyFieldBasedComparator \
-D mapreduce.text.key.comparator.options=-k1 \
-input /tmp/tf-out.tsv/part-00000 \
-output /tmp/df-out.tsv \
-file map-df.py \
-mapper "python map-df.py" \
-file reduce-df.py \
-reducer "python reduce-df.py"

Instead of map-df.py one could use IdentifyMapper

/usr/bin/hadoop  jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
-D map.output.key.field.separator=\t \
-D stream.num.map.output.key.fields=1 \
-D mapreduce.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.KeyFieldBasedComparator \
-D mapreduce.text.key.comparator.options=-k1 \
-input /tmp/tf-out.tsv/part-00000 \
-output /tmp/df-out.tsv \
-mapper org.apache.hadoop.mapred.lib.IdentityMapper \
-file reduce-df.py \
-reducer "python reduce-df.py"


3. calculate TF-IDF

We calculate tf-idf only in the mapper. The number of documents in the collection
is passed as a parameter to tf-idf.py

/usr/bin/hadoop  jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
-D mapreduce.reduce.tasks=0 \
-input /tmp/df-out.tsv/part-00000 \
-output /tmp/tf-idf.out \
-file tf-idf.py \
-mapper "python tf-idf.py 15578" \



