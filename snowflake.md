## Installation on Snowflake

```
USE ROLE accountadmin;
USE WAREHOUSE testwarehouse;
USE DATABASE test_vault_kurs;
USE SCHEMA datalake;

CREATE STORAGE INTEGRATION gcs_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://se-topoofminds-dvic-datalake')
;

GRANT USAGE ON INTEGRATION gcs_integration TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

CREATE FILE FORMAT test_vault_kurs.datalake.jsonl 
  TYPE = 'JSON' 
  COMPRESSION = 'AUTO' 
  ENABLE_OCTAL = FALSE 
  ALLOW_DUPLICATE = FALSE 
  STRIP_OUTER_ARRAY = FALSE 
  STRIP_NULL_VALUES = FALSE 
  IGNORE_UTF8_ERRORS = TRUE
;

CREATE STAGE gcs_datalake
  URL = 'gcs://se-topoofminds-dvic-datalake/'
  STORAGE_INTEGRATION = gcs_integration
  FILE_FORMAT = jsonl
;
  
ls @gcs_datalake/datalake2;
    
CREATE OR REPLACE EXTERNAL TABLE datalake.customer (
     cell_number STRING AS (value:cell_number::STRING)
    ,preferred_payment_type STRING AS (value:preferred_payment_type::STRING)
    ,email STRING AS (value:email::STRING)
    ,occupation STRING AS (value:occupation::STRING)
    ,last_name STRING AS (value:last_name::STRING)
    ,first_name STRING AS (value:first_name::STRING)
    ,ingestion_time TIMESTAMP AS (value:ingestion_time::TIMESTAMP)
    ,date TIMESTAMP AS (value:date::TIMESTAMP)
    ,customer_id INTEGER AS (value:customer_id::INTEGER)
    ,dt DATE AS CAST(SUBSTRING(METADATA$FILENAME, -21, 10) AS DATE)
)
PARTITION BY (dt)
WITH LOCATION = @gcs_datalake/datalake2/customer/
FILE_FORMAT = 'JSONL'
AUTO_REFRESH = FALSE
;

CREATE OR REPLACE EXTERNAL TABLE datalake.customer_address (
     city STRING AS (value:city::STRING)
    ,address STRING AS (value:address::STRING)
    ,country STRING AS (value:country::STRING)
    ,county STRING AS (value:county::STRING)
    ,postal_code INTEGER AS (value:last_name::INTEGER)
    ,ingestion_time TIMESTAMP AS (value:ingestion_time::TIMESTAMP)
    ,date TIMESTAMP AS (value:date::TIMESTAMP)
    ,customer_id INTEGER AS (value:customer_id::INTEGER)
    ,dt DATE AS CAST(SUBSTRING(METADATA$FILENAME, -21, 10) AS DATE)
)
PARTITION BY (dt)
WITH LOCATION = @gcs_datalake/datalake2/customer_address/
FILE_FORMAT = 'JSONL'
AUTO_REFRESH = FALSE
;

CREATE OR REPLACE EXTERNAL TABLE datalake.customer_classes (
     created TIMESTAMP AS (value:created::TIMESTAMP)
    ,description STRING AS (value:description::STRING)
    ,name STRING AS (value:name::STRING)
    ,id STRING AS (value:id::STRING)
    ,dt DATE AS CAST(SUBSTRING(METADATA$FILENAME, -21, 10) AS DATE)
)
PARTITION BY (dt)
WITH LOCATION = @gcs_datalake/datalake2/customer_classes/
FILE_FORMAT = 'JSONL'
AUTO_REFRESH = FALSE
;


CREATE OR REPLACE EXTERNAL TABLE datalake.customer_segmentations (
    ingestion_time TIMESTAMP AS (value:ingestion_time::TIMESTAMP) 
    ,date TIMESTAMP AS (value:date::TIMESTAMP)
    ,customer_class_id STRING AS (value:customer_class_id::STRING)
    ,customer_id INTEGER AS (value:customer_id::INTEGER)
    ,segmentation_id INTEGER AS (value:segmentation_id::INTEGER)
    ,dt DATE AS CAST(SUBSTRING(METADATA$FILENAME, -21, 10) AS DATE)
)
PARTITION BY (dt)
WITH LOCATION = @gcs_datalake/datalake2/customer_segmentations/
FILE_FORMAT = 'JSONL'
AUTO_REFRESH = FALSE
;

CREATE OR REPLACE EXTERNAL TABLE datalake.product (
     price INTEGER AS (value:price::INTEGER)
    ,product_name STRING AS (value:product_name::STRING)
    ,product_type STRING AS (value:product_type::STRING)
    ,created TIMESTAMP AS (value:created::TIMESTAMP)
    ,color STRING AS (value:color::STRING)
    ,ean INTEGER AS (value:ean::INTEGER)
    ,product_id INTEGER AS (value:product_id::INTEGER)
    ,dt DATE AS CAST(SUBSTRING(METADATA$FILENAME, -21, 10) AS DATE)
)
PARTITION BY (dt)
WITH LOCATION = @gcs_datalake/datalake2/product/
FILE_FORMAT = 'JSONL'
AUTO_REFRESH = FALSE
;


CREATE OR REPLACE EXTERNAL TABLE datalake.product_sizes (
     height INTEGER AS (value:height::INTEGER)
    ,created TIMESTAMP AS (value:created::TIMESTAMP)
    ,length INTEGER AS (value:length::INTEGER)
    ,width INTEGER AS (value:width::INTEGER)
    ,weight INTEGER AS (value:weight::INTEGER)
    ,product_id INTEGER AS (value:product_id::INTEGER)
    ,dt DATE AS CAST(SUBSTRING(METADATA$FILENAME, -21, 10) AS DATE)
)
PARTITION BY (dt)
WITH LOCATION = @gcs_datalake/datalake2/product_sizes/
FILE_FORMAT = 'JSONL'
AUTO_REFRESH = FALSE
;

CREATE OR REPLACE EXTERNAL TABLE datalake.sales (
     sales_lines VARIANT AS (value:sales_lines::VARIANT)
    ,total INTEGER AS (value:total::INTEGER)
    ,payment_type STRING AS (value:payment_type::STRING)
    ,customer_id INTEGER AS (value:customer_id::INTEGER)
    ,tax FLOAT AS (value:tax::FLOAT)
    ,ingestion_time TIMESTAMP AS (value:ingestion_time::TIMESTAMP) 
    ,date TIMESTAMP AS (value:date::TIMESTAMP)
    ,transaction_id INTEGER AS (value:transaction_id::INTEGER)
    ,dt DATE AS CAST(SUBSTRING(METADATA$FILENAME, -21, 10) AS DATE)
)
PARTITION BY (dt)
WITH LOCATION = @gcs_datalake/datalake2/sales/
FILE_FORMAT = 'JSONL'
AUTO_REFRESH = FALSE
;
```

### sales_line_stg for Snowflake
Currently the sales_line_stg in models/staging only works with Big query. If running dbt to Snowflake just run:

`dbt run -s product_sizes_stg.sql`

And manually install the below query

```
CREATE OR REPLACE VIEW test_vault_kurs.dv.sales_line_stg
AS
SELECT 
  transaction_id || '|' || index AS sales_line_id
  ,transaction_id
  ,index AS line_offset
  ,sl.value:product_id::INTEGER AS product_id
  ,sl.value:price::INTEGER AS price
  ,sl.value:quantity::INTEGER AS quantity
  ,sl.value:total::INTEGER AS total
  ,date
  ,ingestion_time
  ,dt
FROM 
    datalake.sales s
    ,table(flatten(s.sales_lines)) sl
;
```

### models/bv

Currently a couple of models is coded strictly to Big query. Needs attention for it to be cross db templates.
