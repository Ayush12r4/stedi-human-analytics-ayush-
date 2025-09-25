CREATE EXTERNAL TABLE `step_trainer-landing`(
  `serialnumber` string COMMENT 'from deserializer', 
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://ayush-project-udacity/step_trainer/landing/'
TBLPROPERTIES (
  'classification'='json')
