{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='workflow_run_hk',
        merge_update_columns=['last_seen_datetime'],
        on_schema_change='fail',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
        partitioned_by=['load_date']
    )
}}

{% set source_table = 'metadata' %}

with source as (

    select distinct
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id
    from
        {{ source('tidywigits', source_table) }}

    {% if is_incremental() %}

    where
        regexp_like("$path", '{{ source_table }}\.parquet$')
        and batch_date >= date_format(( select max(load_date) from {{ this }} ), '%Y-%m-%d')

    {% else %}

    where
        regexp_like("$path", '{{ source_table }}\.parquet$')
        and batch_date >= '{{ var("initial_batch_date") }}'

    {% endif %}

),

transformed as (

    select
        lower(to_hex(sha256(cast(portal_run_id as varbinary)))) as workflow_run_hk,
        portal_run_id,
        cast('{{ run_started_at.strftime("%Y-%m-%d") }}' as date) as load_date,
        'tidywigits_{{ source_table }}' as record_source,
        cast('{{ run_started_at }}' as timestamp) as last_seen_datetime
    from
        source
    order by portal_run_id

),

final as (

    select
        cast(workflow_run_hk as varchar(64)) as workflow_run_hk,
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(load_date as date) as load_date,
        cast(record_source as varchar(255)) as record_source,
        cast(last_seen_datetime as timestamp) as last_seen_datetime
    from
        transformed

)

select * from final
