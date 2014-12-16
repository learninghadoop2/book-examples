DROP TABLE IF EXISTS tweets_pagerank;
CREATE EXTERNAL TABLE tweets_pagerank
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.literal'='{
    "type":"record",
    "name":"record",
    "fields": [
        {"name":"topic","type":["null","int"]},
        {"name":"source","type":["null","int"]},
        {"name":"rank","type":["null","float"]}
    ]
}')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${data}/ch5-pagerank';