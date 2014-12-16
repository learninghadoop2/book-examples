add JAR myudfs.jar;
create temporary function string_to_int as 'myudfs.StringToInt';

create table lookuptable (tag string, tag_id bigint);

insert overwrite table lookuptable select a.hashtag, string_to_int(a.hashtag) as tag_id from
(select regexp_extract(text, '(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)') as hashtag  from tweets 
group by regexp_extract(text, '(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)')
) a
group by a.hashtag, string_to_int(a.hashtag);
