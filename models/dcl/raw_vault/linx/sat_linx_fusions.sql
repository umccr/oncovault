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

{% set source_table = 'linx_fusions' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_pfix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        breakendid5,
        breakendid3,
        name,
        reported,
        reported_type,
        reportable_reasons,
        phased,
        likelihood,
        chain_length,
        chain_links,
        chain_terminated,
        domains_kept,
        domains_lost,
        skipped_exons_up,
        skipped_exons_down,
        fused_exon_up,
        fused_exon_down,
        gene_start,
        gene_context_start,
        transcript_start,
        gene_end,
        gene_context_end,
        transcript_end,
        junction_cn

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
                'breakendid5',
                'breakendid3',
                'name',
                'reported',
                'reported_type',
                'reportable_reasons',
                'phased',
                'likelihood',
                'chain_length',
                'chain_links',
                'chain_terminated',
                'domains_kept',
                'domains_lost',
                'skipped_exons_up',
                'skipped_exons_down',
                'fused_exon_up',
                'fused_exon_down',
                'gene_start',
                'gene_context_start',
                'transcript_start',
                'gene_end',
                'gene_context_end',
                'transcript_end',
                'junction_cn'
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

        cast(breakendid5 as varchar) as breakendid5,
        cast(breakendid3 as varchar) as breakendid3,
        cast("name" as varchar) as "name",
        cast(reported as varchar) as reported,
        cast(reported_type as varchar) as reported_type,
        cast(reportable_reasons as varchar) as reportable_reasons,
        cast(phased as varchar) as phased,
        cast(likelihood as varchar) as likelihood,
        cast(chain_length as double) as chain_length,
        cast(chain_links as double) as chain_links,
        cast(chain_terminated as varchar) as chain_terminated,
        cast(domains_kept as varchar) as domains_kept,
        cast(domains_lost as varchar) as domains_lost,
        cast(skipped_exons_up as double) as skipped_exons_up,
        cast(skipped_exons_down as double) as skipped_exons_down,
        cast(fused_exon_up as double) as fused_exon_up,
        cast(fused_exon_down as double) as fused_exon_down,
        cast(gene_start as varchar) as gene_start,
        cast(gene_context_start as varchar) as gene_context_start,
        cast(transcript_start as varchar) as transcript_start,
        cast(gene_end as varchar) as gene_end,
        cast(gene_context_end as varchar) as gene_context_end,
        cast(transcript_end as varchar) as transcript_end,
        cast(junction_cn as double) as junction_cn

    from
        transformed

)

select * from final
