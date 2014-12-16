-- Find influential users with PageRank
%default input 'ch5-twitter-graph'
%default output 'ch5-pagerank'

%default hdfs_path  'hdfs:///jar'
%default local_path '/opt/cloudera/parcels/CDH/lib'
%default hadoop_distro 'cdh5.0.0'

REGISTER $hdfs_path/elephant-bird-core-4.5-SNAPSHOT.jar
REGISTER $hdfs_path/elephant-bird-hadoop-compat-4.5-SNAPSHOT.jar
REGISTER $hdfs_path/elephant-bird-pig-4.5-SNAPSHOT.jar
REGISTER $hdfs_path/myudfs-pig.jar
REGISTER $local_path/pig/lib/json-simple-1.1.jar
REGISTER $local_path/pig/datafu-1.1.0-$hadoop_distro.jar
REGISTER $local_path/pig/piggybank.jar

DEFINE PageRank datafu.pig.linkanalysis.PageRank('dangling_nodes','true');
DEFINE StringToInt com.learninghadoop2.pig.udf.StringToInt();


-- load the Twitter graph created by graph.pig
twitter_graph = LOAD '$input' USING AvroStorage();

-- clean data by removing tweets without an hashtag or that received no reply
twitter_graph_filtered = FILTER twitter_graph by (destination IS NOT NULL) AND (topic IS NOT null);

-- data pareparation
from_to = foreach twitter_graph_filtered {
  GENERATE 
    StringToInt(source) as source_id, 
    StringToInt(destination) as destination_id, 
    StringToInt(topic) as topic_id;
}

-- build granph and calculate PageRank
reply_to = group from_to by (source_id, destination_id, topic_id);
topic_edges = foreach reply_to {
  GENERATE flatten(group), ((double)COUNT(from_to.topic_id)) as w;
}

topic_edges_grouped = GROUP topic_edges by (topic_id, source_id);
topic_edges_grouped = FOREACH topic_edges_grouped {
  GENERATE
    group.topic_id as topic,
    group.source_id as source,
    topic_edges.(destination_id,w) as edges;
}

topic_ranks = FOREACH (GROUP topic_edges_grouped BY topic) {
  GENERATE
    group as topic,
    FLATTEN(PageRank(topic_edges_grouped.(source,edges))) as (source,rank);
}
topic_ranks = FOREACH topic_ranks GENERATE topic, source, rank;

STORE topic_ranks INTO '$output' USING AvroStorage();
