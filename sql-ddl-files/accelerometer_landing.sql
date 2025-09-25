CREATE EXTERNAL TABLE `accelerometer-landing`(
  `x` float COMMENT 'from deserializer', 
  `y` float COMMENT 'from deserializer', 
  `z` float COMMENT 'from deserializer', 
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://ayush-project-udacity/accelerometer/landing/'
TBLPROPERTIES (
  'classification'='json')
