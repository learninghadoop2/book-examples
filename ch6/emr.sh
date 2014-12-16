#!/bin/bash
# create a new cluster and run the graph and pagerank examples

# cluster conf
AMI_VERSION=3.2.0
TOTAL_NUM_NODES=10
INSTANCE_TYPE=m1.xlarge
PIG_VERSION=0.12


BUCKET=
TWEET_DATA=s3://${BUCKET}/tweets.json

# Pig scripts
GRAPH=s3://${BUCKET}/ch5/script/graph.pig
PAGERANK=s3://${BUCKET}/ch5/script/pagerank.pig

# Output dataset
GRAPH_OUT=s3://${BUCKET}/ch5/out/graph-out/
PAGERANK_OUT=s3://${BUCKET}/ch5/out/pagerank-out/

aws emr create-cluster --name "Pig cluster" \
--ami-version ${AMI_VERSION} \
--instance-type ${INSTANCE_TYPE}  \
--instance-count ${TOTAL_NUM_NODES} \
--applications Name=Pig,Args=--version,${PIG_VERSION} \
--log-uri ${BUCKET}/emr-logs \
--steps Type=PIG,Name='Build the Twitter Graph',Args=[-f,${GRAPH},-p,input=${TWEET_DATA},\
-p,output=${GRAPH_OUT},-p,hdfs_path=${BUCKET}/jar,-p,local_path=${BUCKET}/jar,-p] \
Type=PIG,Name='Calculate PageRank',Args=[-f,s3://${PAGERANK},-p,input=${GRAPH_OUT},-p,output=${PAGERANK_OUT},\
-p,output=${GRAPH_OUT},-p,hdfs_path=${BUCKET}/jar,-p,local_path=${BUCKET}/jar] 
