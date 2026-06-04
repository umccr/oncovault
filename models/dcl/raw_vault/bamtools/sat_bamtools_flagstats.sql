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

{% set source_table = 'bamtools_flagstats' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_pfix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        passed_or_failed,
        total,
        "primary",
        secondary,
        suppl,
        dup,
        primary_dup,
        mapped,
        primary_map,
        paired_in_seq,
        read1,
        read2,
        proper_pair,
        both_map,
        singletons,
        matemap_diff,
        matemap_diff_mapq5,
        mapped_pct,
        primary_map_pct,
        proper_pair_pct,
        singletons_pct

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
                'passed_or_failed',
                'total',
                '"primary"',
                'secondary',
                'suppl',
                'dup',
                'primary_dup',
                'mapped',
                'primary_map',
                'paired_in_seq',
                'read1',
                'read2',
                'proper_pair',
                'both_map',
                'singletons',
                'matemap_diff',
                'matemap_diff_mapq5',
                'mapped_pct',
                'primary_map_pct',
                'proper_pair_pct',
                'singletons_pct'
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

        cast(passed_or_failed as varchar) as passed_or_failed,
        cast(total as double) as total,
        cast("primary" as double) as "primary",
        cast(secondary as double) as secondary,
        cast(suppl as double) as suppl,
        cast(dup as double) as dup,
        cast(primary_dup as double) as primary_dup,
        cast(mapped as double) as mapped,
        cast(primary_map as double) as primary_map,
        cast(paired_in_seq as double) as paired_in_seq,
        cast(read1 as double) as read1,
        cast(read2 as double) as read2,
        cast(proper_pair as double) as proper_pair,
        cast(both_map as double) as both_map,
        cast(singletons as double) as singletons,
        cast(matemap_diff as double) as matemap_diff,
        cast(matemap_diff_mapq5 as double) as matemap_diff_mapq5,
        cast(mapped_pct as double) as mapped_pct,
        cast(primary_map_pct as double) as primary_map_pct,
        cast(proper_pair_pct as double) as proper_pair_pct,
        cast(singletons_pct as double) as singletons_pct

    from
        transformed

)

select * from final
