%default input 'tweets.json'
%default output 'ch5-twitter-graph'

%default hdfs_path  'hdfs:///jar'
%default local_path '/opt/cloudera/parcels/CDH/lib/'
%default hadoop_distro 'cdh5.0.0'


REGISTER $hdfs_path/elephant-bird-core-4.5-SNAPSHOT.jar
REGISTER $hdfs_path/elephant-bird-hadoop-compat-4.5-SNAPSHOT.jar
REGISTER $hdfs_path/elephant-bird-pig-4.5-SNAPSHOT.jar
REGISTER $local_path/pig/lib/json-simple-1.1.jar
REGISTER $local_path/pig/datafu-1.1.0-$hadoop_distro.jar
REGISTER $local_path/pig/piggybank.jar

DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE SRS datafu.pig.sampling.SimpleRandomSample('0.2');
DEFINE SHA datafu.pig.hash.SHA();

DEFINE Quantile datafu.pig.stats.Quantile('0.5','0.90','0.95','0.99');

IMPORT 'top_n.macro';

tweets = LOAD '$input' 
            USING  com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

sampled = FOREACH (GROUP tweets ALL) GENERATE FLATTEN(SRS(tweets));

-- project and anonymize data  
from_to_bag = FOREACH tweets {
  dt = $0#'created_at';
  user_id = (chararray)$0#'user'#'id';
  tweet_id = (chararray)$0#'id_str';
  reply_to_tweet = (chararray)$0#'in_reply_to_status_id_str';
  reply_to = (chararray)$0#'in_reply_to_user_id_str';
  place = $0#'place';
  topics = $0#'entities'#'hashtags';

  GENERATE
    CustomFormatToISO(dt, 'EEE MMMM d HH:mm:ss Z y') AS dt,
    SHA((chararray)CONCAT('SALT', user_id)) AS source,  
    SHA(((chararray)CONCAT('SALT', tweet_id))) AS tweet_id,
    ((reply_to_tweet IS NULL) ? NULL : SHA((chararray)CONCAT('SALT', reply_to_tweet))) AS  reply_to_tweet_id,
    ((reply_to IS NULL) ? NULL : SHA((chararray)CONCAT('SALT', reply_to))) AS destination,
    (chararray)place#'country_code' as country,
    FLATTEN(topics) AS topic;
}

-- extract the hashtag text
from_to = FOREACH from_to_bag { 
  GENERATE 
    dt, 
    tweet_id, 
    reply_to_tweet_id, 
    source, 
    destination, 
    country,
    (chararray)topic#'text' AS topic;
}

tweet_hashtag = FOREACH from_to GENERATE tweet_id, topic;
from_to_self_joined = JOIN from_to BY reply_to_tweet_id LEFT, tweet_hashtag BY tweet_id;

-- graph with source topic
twitter_graph = FOREACH from_to_self_joined  { 
    GENERATE
        from_to::dt AS dt,
        from_to::tweet_id AS tweet_id,
        from_to::reply_to_tweet_id AS reply_to_tweet_id,
        from_to::source AS source,
        from_to::destination AS destination,
        from_to::topic AS topic,
        from_to::country AS country,
        tweet_hashtag::topic AS topic_replied;
}

-- Serialize the Twitter graph to Avro
STORE twitter_graph INTO '$output' using AvroStorage;
top_10_topics = top_n(twitter_graph, topic_replied, 10);

-- Quantiles per topic
topics_with_replies_cnt = FOREACH (GROUP twitter_graph BY topic_replied) {
  GENERATE 
    COUNT(twitter_graph) as cnt;
}

quantiles = FOREACH (GROUP topics_with_replies_cnt ALL) {
    sorted = ORDER topics_with_replies_cnt BY cnt;
    GENERATE Quantile(sorted);
 }
