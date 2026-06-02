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

{% set source_table = 'purple_cnvgenetsv' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_pfix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        chrom,
        "start",
        "end",
        gene,
        cn_min,
        cn_max,
        somatic_regions,
        transcript_id,
        is_canonical,
        chrom_band,
        regions_min,
        start_region_min,
        end_region_min,
        start_region_min_support,
        end_region_min_support,
        region_min_method,
        cn_minor_allele_min,
        window_count_depth,
        gc_content

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
                'chrom',
                '"start"',
                '"end"',
                'gene',
                'cn_min',
                'cn_max',
                'somatic_regions',
                'transcript_id',
                'is_canonical',
                'chrom_band',
                'regions_min',
                'start_region_min',
                'end_region_min',
                'start_region_min_support',
                'end_region_min_support',
                'region_min_method',
                'cn_minor_allele_min',
                'window_count_depth',
                'gc_content'
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

        cast(chrom as varchar) as chrom,
        cast("start" as double) as "start",
        cast("end" as double) as "end",
        cast(gene as varchar) as gene,
        cast(cn_min as double) as cn_min,
        cast(cn_max as double) as cn_max,
        cast(somatic_regions as double) as somatic_regions,
        cast(transcript_id as varchar) as transcript_id,
        cast(is_canonical as varchar) as is_canonical,
        cast(chrom_band as varchar) as chrom_band,
        cast(regions_min as double) as regions_min,
        cast(start_region_min as double) as start_region_min,
        cast(end_region_min as double) as end_region_min,
        cast(start_region_min_support as varchar) as start_region_min_support,
        cast(end_region_min_support as varchar) as end_region_min_support,
        cast(region_min_method as varchar) as region_min_method,
        cast(cn_minor_allele_min as double) as cn_minor_allele_min,
        cast(window_count_depth as double) as window_count_depth,
        cast(gc_content as double) as gc_content

    from
        transformed

)

select * from final
