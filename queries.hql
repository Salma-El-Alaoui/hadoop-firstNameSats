/**
	create an external table from the csv file
**/
USE selalaouitalibi;
DROP TABLE IF EXISTS prenoms;
CREATE EXTERNAL TABLE prenoms(
name string,
gender array<string>,
origin array<string>,
version double)
row format delimited
fields terminated by '\073'
collection items terminated by ","
stored AS textfile location "/user/selalaouitalibi/prenoms-db/";

/**
	create the ORC table
**/
DROP TABLE IF EXISTS prenoms_opt;
CREATE TABLE prenoms_opt(
name string,
gender array<string>,
origin array<string>,
version double)
row format delimited
fields terminated by '\073'
collection items terminated by ","
stored AS ORC;

/**
	Insert the data from the external table to the Hive ORC table
**/
INSERT OVERWRITE TABLE prenoms_opt SELECT * FROM prenoms;

/**
	count first name as origin
**/
SELECT trim(origin_exploded), count(1) 
FROM prenoms_opt lateral view explode(origin) view_exp AS origin_exploded 
GROUP BY trim(origin_exploded);

/**
	count number of first names by number of origins 
**/
SELECT size(origin) AS number_origins, count(1)
FROM prenoms_opt
GROUP BY size(origin);

/**
	Proportion (in%) of males and females 
**/
SELECT q1.gender_key, q1.absolute_count/q2.total * 100 AS proportion
FROM
(
SELECT trim(gender_exploded) AS gender_key, count(1) AS absolute_count
FROM prenoms_opt lateral view explode(gender) view_exp AS gender_exploded
GROUP BY trim(gender_exploded)
) q1,
(
SELECT count(*) AS total
FROM prenoms_opt
) q2;
