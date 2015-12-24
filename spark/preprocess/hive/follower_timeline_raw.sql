CREATE EXTERNAL TABLE tchug.follower_timeline_raw (timeline_json string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/data/tchug/';