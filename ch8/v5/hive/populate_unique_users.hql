use ${dbName};

insert overwrite table unique_users
select distinct user_id, name, description, screen_name
from users_hcat ;

