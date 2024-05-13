{{ 
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge', 
    pre_hook=["{{ create_bronze_cvp_media_table() }}",

              "{{ databricks_copy_into(target_table='cvp_media_bronze',
                    source='abfss://deltalake@oneenvadls.dfs.core.windows.net/irfan/n10/raw/magnite/',
                    file_format='csv',
                    expression_list = 'id::int,title,day::date, hour::int,now() AS ingest_timestamp',
                    format_options={'header' : 'true', 'mergeSchema' : 'true','multiLine' : 'true'}
                    ) }}"
    ],
    post_hook=[
        "OPTIMIZE {{ this }}",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
  ) 
}}


WITH de_dup (
SELECT 
      id,
      case
        when title ilike '%fast%' then 'fast'
        when title ilike '%slow%' then 'slow'
        else 'other'
     end as title,
     day,
     hour,
      ingest_timestamp,
       ROW_NUMBER() OVER(PARTITION BY id ORDER BY ingest_timestamp DESC) AS DupRank
      FROM {{target.catalog}}.{{var('bronze_schema')}}.cvp_media_bronze
      -- Add Incremental Processing Macro here
      {% if is_incremental() %}

        WHERE ingest_timestamp > (SELECT MAX(ingest_timestamp) FROM {{ this }})

      {% endif %}
)              
SELECT * except(duprank)
FROM de_dup
WHERE DupRank = 1

-- add escape ='"' to the format_options later.