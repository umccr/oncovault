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

{% set source_table = 'linx_visfusion' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        cluster_id,
        reportable,
        gene_name_up,
        transcript_up,
        chr_up,
        pos_up,
        strand_up,
        region_type_up,
        fused_exon_up,
        gene_name_down,
        transcript_down,
        chr_down,
        pos_down,
        strand_down,
        region_type_down,
        fused_exon_down

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
                'reportable',
                'gene_name_up',
                'transcript_up',
                'chr_up',
                'pos_up',
                'strand_up',
                'region_type_up',
                'fused_exon_up',
                'gene_name_down',
                'transcript_down',
                'chr_down',
                'pos_down',
                'strand_down',
                'region_type_down',
                'fused_exon_down'
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
        cast(reportable as varchar) as reportable,

        cast(gene_name_up as varchar) as gene_name_up,
        cast(transcript_up as varchar) as transcript_up,
        cast(chr_up as varchar) as chr_up,
        cast(pos_up as varchar) as pos_up,
        cast(strand_up as varchar) as strand_up,
        cast(region_type_up as varchar) as region_type_up,
        cast(fused_exon_up as varchar) as fused_exon_up,

        cast(gene_name_down as varchar) as gene_name_down,
        cast(transcript_down as varchar) as transcript_down,
        cast(chr_down as varchar) as chr_down,
        cast(pos_down as varchar) as pos_down,
        cast(strand_down as varchar) as strand_down,
        cast(region_type_down as varchar) as region_type_down,
        cast(fused_exon_down as varchar) as fused_exon_down

    from
        transformed

)

select * from final
