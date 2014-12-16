CREATE DATABASE IF NOT EXISTS ${dbName};

use ${dbName};

drop table if exists tweets;
create external table tweets (
created_at string,
tweet_id string,
text string,
in_reply_to string,
retweeted boolean,
user_id string,
place_id string
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u0001'
STORED AS TEXTFILE
LOCATION '${ingestDir}/tweets';

drop table if exists user;
create external table user (
created_at string,
user_id string,
`location` string,
name string,
description string,
followers_count bigint,
friends_count bigint,
favourites_count bigint,
screen_name string,
listed_count bigint
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${ingestDir}/users';

drop table if exists place;
create external table place (
place_id string,
country_code string,
country string,
`name` string,
full_name string,
place_type string
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${ingestDir}/places';

