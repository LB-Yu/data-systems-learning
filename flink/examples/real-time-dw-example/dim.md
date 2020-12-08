```sql
-- -------------------------
--   省份
--   kafka Source
-- ------------------------- 
DROP TABLE IF EXISTS `ods_base_province`;
CREATE TABLE `ods_base_province` (
  `id` INT,
  `name` STRING,
  `region_id` INT ,
  `area_code`STRING
) WITH(
'connector' = 'kafka',
 'topic' = 'mydw_base_province',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'canal-json' ,
 'scan.startup.mode' = 'earliest-offset' 
) ; 

-- -------------------------
--   省份
--   MySQL Sink
-- ------------------------- 
DROP TABLE IF EXISTS `base_province`;
CREATE TABLE `base_province` (
    `id` INT,
    `name` STRING,
    `region_id` INT ,
    `area_code`STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/dim',
    'table-name' = 'base_province', -- MySQL中的待插入数据的表
    'driver' = 'com.mysql.jdbc.Driver',
    'username' = 'root',
    'password' = '0407',
    'sink.buffer-flush.interval' = '1s'
);

-- -------------------------
--   省份
--   MySQL Sink Load Data
-- ------------------------- 
INSERT INTO base_province
SELECT *
FROM ods_base_province;
```