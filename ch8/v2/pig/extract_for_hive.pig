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
	(chararray)CustomFormatToISO($0#'created_at', 'EEE MMMM d HH:mm:ss Z y') as dt, 
	(chararray)$0#'id_str', (chararray)$0#'text' as text, 
	(chararray)$0#'in_reply_to', (boolean)$0#'retweeted' as is_retweeted, 
	(chararray)$0#'user'#'id_str' as user_id, (chararray)$0#'place'#'id' as place_id;

store tweets_tsv into '$outputDir/tweets' using PigStorage('\u0001');

-- Places
needed_fields = FOREACH tweets generate (chararray)CustomFormatToISO($0#'created_at', 'EEE MMMM d HH:mm:ss Z y') as dt, 
	(chararray)$0#'id_str' as id_str, $0#'place' as place;

place_fields = foreach needed_fields generate 
	(chararray)place#'id' as place_id, 
	(chararray)place#'country_code' as co, 
	(chararray)place#'country' as country, 
	(chararray)place#'name' as place_name, 
	(chararray)place#'full_name' as place_full_name, 
	(chararray)place#'place_type' as place_type;

filtered_places = filter place_fields by co != '';
unique_places = distinct filtered_places;

store unique_places into '$outputDir/places' using PigStorage('\u0001');

-- Users
users = FOREACH tweets generate (chararray)CustomFormatToISO($0#'created_at', 'EEE MMMM d HH:mm:ss Z y') as dt, 
	(chararray)$0#'id_str' as id_str, $0#'user' as user;

user_fields = foreach users generate 
	(chararray)CustomFormatToISO(user#'created_at', 'EEE MMMM d HH:mm:ss Z y') as dt,
	(chararray)user#'id_str' as user_id, 
	(chararray)user#'location' as user_location, 
	(chararray)user#'name' as user_name, 
	(chararray)user#'description' as user_description, 
	(int)user#'followers_count' as followers_count, 
	(int)user#'friends_count' as friends_count, 
	(int)user#'favourites_count' as favourites_count, 
	(chararray)user#'screen_name' as screen_name, 
	(int)user#'listed_count' as listed_count
;

unique_users = distinct user_fields;

store unique_users into '$outputDir/users' using PigStorage('\u0001');
