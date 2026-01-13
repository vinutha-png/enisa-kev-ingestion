-- Archive table for ENISA Known Exploited Vulnerabilities
CREATE EXTERNAL TABLE IF NOT EXISTS enisa_kev_archive (
  cve_id STRING,
  euvd_id STRING,
  vendor_project STRING,
  product STRING,
  vulnerability_name STRING,
  date_reported STRING,
  origin_source STRING,
  short_description STRING,
  exploitation_type STRING,
  threat_actors_exploiting STRING,
  notes STRING,
  cwes STRING
)
PARTITIONED BY (
  dbdate STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{{ params.incoming }}/enisa_kev/'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'serialization.null.format'=''
);
