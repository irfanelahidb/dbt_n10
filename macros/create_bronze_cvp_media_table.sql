{% macro create_bronze_cvp_media_table() %}

-- Seprately DDL creation for doing things like custom / rigid schema DDL or Identity columns
-- can think of making this dynamic as well? e.g. pass table name. but how will you pass column names?
-- also this isn't expected to run every time as the bronze table will exist.

CREATE TABLE IF NOT EXISTS {{target.catalog}}.{{var("bronze_schema")}}.cvp_media_bronze
(
id int,
title STRING,
day date,
hour int,
ingest_timestamp TIMESTAMP
)

{% endmacro %}