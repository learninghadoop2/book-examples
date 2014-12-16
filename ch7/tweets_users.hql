set hive.cli.print.header=true;

select user_id, count(*) as cnt from tweets group by user_id order by cnt desc limit 10;

select tweets.user_id, u.name, count(*) as cnt 
from tweets 
join (select user_id, name from user group by user_id, name) u
on u.user_id  = tweets.user_id
group by tweets.user_id, u.name order by cnt desc limit 10; 

select tweets.user_id, u.name, count(tweets.user_id) as cnt 
from tweets 
join (select user_id, name from user group by user_id, name) u
on u.user_id  = tweets.user_id
group by tweets.user_id, u.name order by cnt desc limit 10; 
