CREATE TABLE tchug.follower_timeline as
SELECT
get_json_object(timeline_json, '$.created_at') AS timeCreatedUTC
,get_json_object(timeline_json, '$.user.utc_offset') AS timeOffset
-- remove all non-space whitespace (or remove delimiter collisions)
,regexp_replace(regexp_replace(get_json_object(timeline_json, '$.text'),'\n',' '),'\t',' ') AS tweetText
,get_json_object(timeline_json, '$.favorite_count') AS favoriteCount
,get_json_object(timeline_json, '$.retweet_count') AS retweetCount
,get_json_object(timeline_json, '$.lang') AS language
,get_json_object(timeline_json, '$.place') AS place
,get_json_object(timeline_json, '$.user.screen_name') AS twitterHandle
,get_json_object(timeline_json, '$.user.lang') AS accountLocation
,get_json_object(timeline_json, '$.user.name') AS accountName
,get_json_object(timeline_json, '$.source') AS source
,get_json_object(timeline_json, '$.coordinates') AS geoCoords
FROM tchug.follower_timeline_raw;