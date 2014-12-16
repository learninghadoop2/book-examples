%default input 'tweets.json'

%default hdfs_path  'hdfs:///jar'
%default local_path '/opt/cloudera/parcels/CDH/lib'
%default hadoop_distro 'cdh5.0.0'

REGISTER $hdfs_path/elephant-bird-core-4.5-SNAPSHOT.jar
REGISTER $hdfs_path/elephant-bird-hadoop-compat-4.5-SNAPSHOT.jar
REGISTER $hdfs_path/elephant-bird-pig-4.5-SNAPSHOT.jar
REGISTER $local_path/pig/lib/json-simple-1.1.jar
REGISTER $local_path/pig/datafu-1.1.0-$hadoop_distro.jar
REGISTER $local_path/pig/piggybank.jar


DEFINE Sessionize datafu.pig.sessions.Sessionize('15m');
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();

IMPORT 'count_rows.macro';
IMPORT 'top_n.macro';

-- load json data
tweets = LOAD '$input' using  com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

-- top 10 hashtags 
tweets_en = FILTER tweets by $0#'lang' == 'en';
hashtags_bag = FOREACH tweets_en generate flatten($0#'entities'#'hashtags') as tag;
hashtags = FOREACH hashtags_bag generate tag#'text' as tag;
top_10_hashtags = top_n(hashtags, tag, 10);

-- hashtag_grpd = GROUP hashtags by tag;
-- hashtag_count = FOREACH hashtag_grpd generate flatten(group), COUNT(hashtags) as number_of_mentions;
-- hashtag_count_sorted = order hashtag_count by number_of_mentions desc;
-- hashtag_top_10 = limit hashtag_count_sorted 10;

-- Top N hashtags per country
hashtags_country_bag = FOREACH tweets {
    generate 
        $0#'place' as place, 
        FLATTEN($0#'entities'#'hashtags') as tag;
}

hashtags_country = FOREACH hashtags_country_bag {
    GENERATE 
        place#'country_code' as co, 
        tag#'text' as tag;
}

hashtags_country_frequency = FOREACH (GROUP hashtags_country ALL)  {
    GENERATE 
        FLATTEN(group), 
        COUNT(hashtags_country) as cnt;
}

hashtags_country_regrouped= GROUP hashtags_country_frequency BY cnt; 
top_results = FOREACH hashtags_country_regrouped {
    result = TOP(10, 1, hashtags_country_frequency);
    GENERATE FLATTEN(result);
}

-- date time manipulation
hourly_tweets = FOREACH tweets {
    GENERATE 
        GetHour(
            ToDate(
                CustomFormatToISO($0#'created_at', 'EEE MMMM d HH:mm:ss Z y'
                    )
            )
        ) as hour;
}
hourly_tweets_count =  FOREACH (GROUP hourly_tweets BY hour) { 
    GENERATE FLATTEN(group), COUNT(hourly_tweets);
}

-- sessions example
users_activity = FOREACH tweets {
      GENERATE 
        CustomFormatToISO($0#'created_at', 'EEE MMMM d HH:mm:ss Z y') AS dt,
        (chararray)$0#'user'#'id' as user_id;
}

users_activity_sessionized = FOREACH (GROUP users_activity BY user_id) {
  ordered = ORDER users_activity BY dt;
  GENERATE FLATTEN(Sessionize(ordered))
           AS (dt, user_id, session_id);
}