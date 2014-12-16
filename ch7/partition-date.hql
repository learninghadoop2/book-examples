drop table partitioned_user;
CREATE TABLE partitioned_user (
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
)  PARTITIONED BY (created_at_date string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u0001'
STORED AS TEXTFILE;

INSERT INTO TABLE partitioned_user
PARTITION( created_at_date = '2014-01-01')
SELECT 
created_at,
user_id,
location,
name,
description,
followers_count,
friends_count,
favourites_count,
screen_name,
listed_count
FROM user;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO TABLE partitioned_user
PARTITION( created_at_date )
SELECT 
created_at,
user_id,
location,
name,
description,
followers_count,
friends_count,
favourites_count,
screen_name,
listed_count,
to_date(created_at) as created_at_date
FROM user;
