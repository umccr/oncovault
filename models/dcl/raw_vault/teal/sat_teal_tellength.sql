{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['library_workflow_run_hk', 'load_date', 'hash_diff'],
        merge_update_columns=['last_seen_datetime'],
        on_schema_change='fail',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
        partitioned_by=['load_date']
    )
}}

{% set source_table = 'teal_tellength' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        sample_id,
        type,
        tel_length_raw,
        tel_length_final,
        fragments_full,
        fragments_c_rich_partial,
        fragments_g_rich_partial,
        reads_telomeric_total,
        purity,
        ploidy,
        dup_prop,
        dp_read_mean,
        dp_read_gc50

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

encoded as (

    select
        lower(to_hex(sha256(cast(portal_run_id as varbinary)))) as workflow_run_hk,
        lower(to_hex(sha256(cast(library_id as varbinary)))) as library_hk,
        {{
            generate_hash_diff([
                'batch_id',
                'batch_date',
                'sample_id',
                'type',
                'tel_length_raw',
                'tel_length_final',
                'fragments_full',
                'fragments_c_rich_partial',
                'fragments_g_rich_partial',
                'reads_telomeric_total',
                'purity',
                'ploidy',
                'dup_prop',
                'dp_read_mean',
                'dp_read_gc50'
            ])
        }} as hash_diff,
        *
    from
        source

),

transformed as (

    select
        lower(to_hex(sha256(cast(concat(workflow_run_hk, library_hk) as varbinary)))) as library_workflow_run_hk,
        cast('{{ run_started_at.strftime("%Y-%m-%d") }}' as date) as load_date,
        'tidywigits_{{ source_table }}' as record_source,
        cast('{{ run_started_at }}' as timestamp) as last_seen_datetime,
        *
    from
        encoded

),

final as (

    select
        cast(library_workflow_run_hk as varchar(64)) as library_workflow_run_hk,
        cast(load_date as date) as load_date,
        cast(hash_diff as varchar(64)) as hash_diff,
        cast(record_source as varchar(255)) as record_source,
        cast(last_seen_datetime as timestamp) as last_seen_datetime,
        cast(batch_id as varchar(26)) as batch_id,
        cast(batch_date as date) as batch_date,

        cast(sample_id as varchar) as sample_id,
        cast("type" as varchar) as "type",
        cast(tel_length_raw as double) as tel_length_raw,
        cast(tel_length_final as double) as tel_length_final,
        cast(fragments_full as double) as fragments_full,
        cast(fragments_c_rich_partial as double) as fragments_c_rich_partial,
        cast(fragments_g_rich_partial as double) as fragments_g_rich_partial,
        cast(reads_telomeric_total as double) as reads_telomeric_total,
        cast(purity as double) as purity,
        cast(ploidy as double) as ploidy,
        cast(dup_prop as double) as dup_prop,
        cast(dp_read_mean as double) as dp_read_mean,
        cast(dp_read_gc50 as double) as dp_read_gc50

    from
        transformed

)

select * from final
