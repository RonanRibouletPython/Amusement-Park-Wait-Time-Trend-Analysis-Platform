CREATE OR REPLACE EXTERNAL TABLE `amusement-park-wait-time.amusement_park_raw.queue_times`
(
  -- Data Content
  park_id INT64,
  lands JSON,
  rides JSON
)
WITH PARTITION COLUMNS (
  -- Partition Keys (Folder structure)
  year STRING,
  month STRING,
  day STRING,
  hour STRING,
  minute STRING
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://amusement-park-datalake-v1/layer=bronze/source=queue_times/*'],
  hive_partition_uri_prefix = 'gs://amusement-park-datalake-v1/layer=bronze/source=queue_times/',
  ignore_unknown_values = TRUE
);