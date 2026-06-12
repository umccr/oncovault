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

{% set source_table = 'purple_germdeltsv' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        gene,
        chrom,
        chrom_band,
        start_region,
        end_region,
        window_count_depth,
        start_exon,
        end_exon,
        detection_method,
        status_germline,
        status_tumor,
        cn_germline,
        cn_tumor,
        filter,
        cohort_frequency,
        reported

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
                'gene',
                'chrom',
                'chrom_band',
                'start_region',
                'end_region',
                'window_count_depth',
                'start_exon',
                'end_exon',
                'detection_method',
                'status_germline',
                'status_tumor',
                'cn_germline',
                'cn_tumor',
                'filter',
                'cohort_frequency',
                'reported'
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

        cast(gene as varchar) as gene,
        cast(chrom as varchar) as chrom,
        cast(chrom_band as varchar) as chrom_band,
        cast(start_region as double) as start_region,
        cast(end_region as double) as end_region,
        cast(window_count_depth as double) as window_count_depth,
        cast(start_exon as double) as start_exon,
        cast(end_exon as double) as end_exon,
        cast(detection_method as varchar) as detection_method,
        cast(status_germline as varchar) as status_germline,
        cast(status_tumor as varchar) as status_tumor,
        cast(cn_germline as double) as cn_germline,
        cast(cn_tumor as double) as cn_tumor,
        cast(filter as varchar) as filter,
        cast(cohort_frequency as double) as cohort_frequency,
        cast(reported as varchar) as reported

    from
        transformed

)

select * from final
