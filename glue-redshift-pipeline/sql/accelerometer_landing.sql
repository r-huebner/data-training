CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='timestamp,user,x,y,z') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://buckettestudacity/accelerometer/landing/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='b1cf41c8-f357-4abf-b7bc-83c462cdd37d', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='accelerometer_landing', 
  'averageRecordSize'='761', 
  'classification'='json', 
  'compressionType'='none', 
  'objectCount'='9', 
  'recordCount'='9007', 
  'sizeKey'='6871328', 
  'typeOfData'='file')