REGISTER /opt/cloudera/parcels/CDH/lib/pig/piggybank.jar
REGISTER  /opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-1.1.jar

REGISTER hdfs:///jar/elephant-bird-pig-4.5.jar;
REGISTER hdfs:///jar/elephant-bird-hadoop-compat-4.5.jar; 
REGISTER hdfs:///jar/elephant-bird-core-4.5.jar;

DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
-- use PigStorage from piggybank
-- DEFINE PigStorage org.apache.pig.piggybank.storage.avro.PigStorage();

-- load json data
tweets = LOAD '$inputDir' using  com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

-- Tweets
tweets_tsv = foreach tweets generate
        (chararray)CustomFormatToISO($0#'created_at', 'EEE MMMM d HH:mm:ss Z y') as created_at,
        (chararray)$0#'id_str' as tweet_id_str, (chararray)$0#'text' as text,
        (chararray)$0#'in_reply_to' as in_reply_to, (chararray)$0#'retweeted' as is_retweeted,
        (chararray)$0#'user'#'id_str' as user_id, (chararray)$0#'place'#'id' as place_id;

store tweets_tsv into 'twttr.tweets_hcat' using org.apache.hive.hcatalog.pig.HCatStorer('partition_key=$partitionKey');

-- Places
needed_fields = FOREACH tweets generate (chararray)CustomFormatToISO($0#'created_at', 'EEE MMMM d HH:mm:ss Z y') as created_at, 
	(chararray)$0#'id_str' as id_str, $0#'place' as place;

place_fields = foreach needed_fields generate 
	(chararray)place#'id' as place_id, 
	(chararray)place#'country_code' as country_code, 
	(chararray)place#'country' as country, 
	(chararray)place#'name' as name, 
	(chararray)place#'full_name' as full_name, 
	(chararray)place#'place_type' as place_type;

filtered_places = filter place_fields by country_code != '';
unique_places = distinct filtered_places;

store unique_places into 'twttr.places_hcat' using org.apache.hive.hcatalog.pig.HCatStorer('partition_key=$partitionKey');

-- Users
users = FOREACH tweets generate (chararray)CustomFormatToISO($0#'created_at', 'EEE MMMM d HH:mm:ss Z y') as created_at, 
	(chararray)$0#'id_str' as id_str, $0#'user' as user;

user_fields = foreach users generate 
	(chararray)CustomFormatToISO(user#'created_at', 'EEE MMMM d HH:mm:ss Z y') as created_at,
	(chararray)user#'id_str' as user_id, 
	(chararray)user#'location' as location, 
	(chararray)user#'name' as name, 
	(chararray)user#'description' as description, 
	(long)user#'followers_count' as followers_count, 
	(long)user#'friends_count' as friends_count, 
	(long)user#'favourites_count' as favourites_count, 
	(chararray)user#'screen_name' as screen_name, 
	(long)user#'listed_count' as listed_count
;

unique_users = distinct user_fields;

store unique_users into 'twttr.users_hcat' using org.apache.hive.hcatalog.pig.HCatStorer('partition_key=$partitionKey');
