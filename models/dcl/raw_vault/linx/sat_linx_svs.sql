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

{% set source_table = 'linx_svs' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        vcf_id,
        sv_id,
        cluster_id,
        cluster_reason,
        fragile_site_start,
        fragile_site_end,
        is_foldback,
        linetype_start,
        linetype_end,
        junction_cn_min,
        junction_cn_max,
        gene_start,
        gene_end,
        local_topology_id_start,
        local_topology_id_end,
        local_topology_start,
        local_topology_end,
        local_ti_count_start,
        local_ti_count_end

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
                'vcf_id',
                'sv_id',
                'cluster_id',
                'cluster_reason',
                'fragile_site_start',
                'fragile_site_end',
                'is_foldback',
                'linetype_start',
                'linetype_end',
                'junction_cn_min',
                'junction_cn_max',
                'gene_start',
                'gene_end',
                'local_topology_id_start',
                'local_topology_id_end',
                'local_topology_start',
                'local_topology_end',
                'local_ti_count_start',
                'local_ti_count_end'
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

        cast(vcf_id as varchar) as vcf_id,
        cast(sv_id as varchar) as sv_id,
        cast(cluster_id as varchar) as cluster_id,
        cast(cluster_reason as varchar) as cluster_reason,
        cast(fragile_site_start as varchar) as fragile_site_start,
        cast(fragile_site_end as varchar) as fragile_site_end,
        cast(is_foldback as varchar) as is_foldback,
        cast(linetype_start as varchar) as linetype_start,
        cast(linetype_end as varchar) as linetype_end,
        cast(junction_cn_min as double) as junction_cn_min,
        cast(junction_cn_max as double) as junction_cn_max,
        cast(gene_start as varchar) as gene_start,
        cast(gene_end as varchar) as gene_end,
        cast(local_topology_id_start as varchar) as local_topology_id_start,
        cast(local_topology_id_end as varchar) as local_topology_id_end,
        cast(local_topology_start as varchar) as local_topology_start,
        cast(local_topology_end as varchar) as local_topology_end,
        cast(local_ti_count_start as double) as local_ti_count_start,
        cast(local_ti_count_end as double) as local_ti_count_end

    from
        transformed

)

select * from final
