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

{% set source_table = 'bamtools_summary_stats' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        tot_region_bases,
        tot_reads,
        dup_reads,
        dual_strand_reads,
        cov_mean,
        cov_sd,
        cov_median,
        cov_mad,
        lowmapq_pct,
        dup_pct,
        unpaired_pct,
        lowbaseq_pct,
        overlap_read_pct,
        cov_capped

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
                'tot_region_bases',
                'tot_reads',
                'dup_reads',
                'dual_strand_reads',
                'cov_mean',
                'cov_sd',
                'cov_median',
                'cov_mad',
                'lowmapq_pct',
                'dup_pct',
                'unpaired_pct',
                'lowbaseq_pct',
                'overlap_read_pct',
                'cov_capped'
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

        cast(tot_region_bases as double) as tot_region_bases,
        cast(tot_reads as double) as tot_reads,
        cast(dup_reads as double) as dup_reads,
        cast(dual_strand_reads as double) as dual_strand_reads,
        cast(cov_mean as double) as cov_mean,
        cast(cov_sd as double) as cov_sd,
        cast(cov_median as double) as cov_median,
        cast(cov_mad as double) as cov_mad,
        cast(lowmapq_pct as double) as lowmapq_pct,
        cast(dup_pct as double) as dup_pct,
        cast(unpaired_pct as double) as unpaired_pct,
        cast(lowbaseq_pct as double) as lowbaseq_pct,
        cast(overlap_read_pct as double) as overlap_read_pct,
        cast(cov_capped as double) as cov_capped

    from
        transformed

)

select * from final
