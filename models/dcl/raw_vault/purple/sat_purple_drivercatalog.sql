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

{% set source_table = 'purple_drivercatalog' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_pfix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        chrom,
        chrom_band,
        gene,
        transcript,
        is_canonical,
        driver,
        category,
        likelihood_method,
        driver_likelihood,
        missense,
        nonsense,
        splice,
        inframe,
        frameshift,
        biallelic,
        cn_min,
        cn_max

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
                'chrom_band',
                'gene',
                'transcript',
                'is_canonical',
                'driver',
                'category',
                'likelihood_method',
                'driver_likelihood',
                'missense',
                'nonsense',
                'splice',
                'inframe',
                'frameshift',
                'biallelic',
                'cn_min',
                'cn_max'
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
        cast(chrom_band as varchar) as chrom_band,
        cast(gene as varchar) as gene,
        cast(transcript as varchar) as transcript,
        cast(is_canonical as varchar) as is_canonical,
        cast(driver as varchar) as driver,
        cast(category as varchar) as category,
        cast(likelihood_method as varchar) as likelihood_method,
        cast(driver_likelihood as double) as driver_likelihood,
        cast(missense as double) as missense,
        cast(nonsense as double) as nonsense,
        cast(splice as double) as splice,
        cast(inframe as double) as inframe,
        cast(frameshift as double) as frameshift,
        cast(biallelic as varchar) as biallelic,
        cast(cn_min as double) as cn_min,
        cast(cn_max as double) as cn_max

    from
        transformed

)

select * from final
