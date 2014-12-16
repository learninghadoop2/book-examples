set -e
# Update this file before sourcing it
source twitter.keys
python stream.py -j -n 100 > /tmp/tweets.out
hdfs dfs -put /tmp/tweets.out /tmp/tweets/tweets.out
rm -f /tmp/tweets.out
