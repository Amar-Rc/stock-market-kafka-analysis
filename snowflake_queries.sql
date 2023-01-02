/*create database and schema for the project*/

create database kafka_db;
create schema kafka_schema;


/*creating integration object to connect to AWS S3*/

create or replace storage integration s3_int
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = '<iam role arn>'
storage_allowed_locations = ('s3://kafka-stock-market-tutorial-youtube-amar/')
comment = 'Integrating snowflake to s3';


/*creating stage to read data from s3 bucket*/

CREATE OR REPLACE stage MANAGE_DB.external_stages.kafka_data
url = 's3://kafka-stock-market-tutorial-youtube-amar/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = MANAGE_DB.file_formats.JSON_FILEFORMAT;


/*create table to load data*/

create or replace table kafka_db.kafka_schema.kafka_table (
index varchar,
date date,
open float,
high float,
low float,
close float,
adj_close float,
volume float,
close_usd float
);



/*creating pipe to read any new file coming to the S3 path via kafka. once pipe is created, need to create sqs event notification in S3 bucket*/

CREATE OR REPLACE pipe MANAGE_DB.pipes.kafka_pipe
auto_ingest = TRUE
AS
COPY INTO kafka_db.kafka_schema.kafka_table
FROM (select $1:Index::string,
$1:Date::date,
$1:Open::float,
$1:High::float,
$1:Low::float,
$1:Close::float,
$1:"Adj Close"::float,
$1:Volume::float,
$1:CloseUSD::float
from @MANAGE_DB.external_stages.kafka_data);


/*get sqs notification id for S3 event notification (column name: notification_channel)*/

desc pipe MANAGE_DB.pipes.kafka_pipe;


/*checking state of pipe. It should be running*/

SELECT SYSTEM$PIPE_STATUS('MANAGE_DB.pipes.kafka_pipe');


select count(1) from kafka_table;


