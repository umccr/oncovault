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

{% set source_table = 'linx_links' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_pfix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        cluster_id,
        chain_id,
        chain_index,
        chain_count,
        lower_sv_id,
        upper_sv_id,
        lower_breakend_is_start,
        upper_breakend_is_start,
        chrom,
        arm,
        assembled,
        traversed_sv_count,
        length,
        junction_cn,
        junction_cn_uncertainty,
        pseudogene_info,
        ecdna

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
                'cluster_id',
                'chain_id',
                'chain_index',
                'chain_count',
                'lower_sv_id',
                'upper_sv_id',
                'lower_breakend_is_start',
                'upper_breakend_is_start',
                'chrom',
                'arm',
                'assembled',
                'traversed_sv_count',
                'length',
                'junction_cn',
                'junction_cn_uncertainty',
                'pseudogene_info',
                'ecdna'
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

        cast(cluster_id as varchar) as cluster_id,
        cast(chain_id as varchar) as chain_id,
        cast(chain_index as varchar) as chain_index,
        cast(chain_count as double) as chain_count,
        cast(lower_sv_id as varchar) as lower_sv_id,
        cast(upper_sv_id as varchar) as upper_sv_id,
        cast(lower_breakend_is_start as varchar) as lower_breakend_is_start,
        cast(upper_breakend_is_start as varchar) as upper_breakend_is_start,
        cast(chrom as varchar) as chrom,
        cast(arm as varchar) as arm,
        cast(assembled as varchar) as assembled,
        cast(traversed_sv_count as double) as traversed_sv_count,
        cast("length" as double) as "length",
        cast(junction_cn as double) as junction_cn,
        cast(junction_cn_uncertainty as double) as junction_cn_uncertainty,
        cast(pseudogene_info as varchar) as pseudogene_info,
        cast(ecdna as varchar) as ecdna

    from
        transformed

)

select * from final
