{{
    config(
        on_schema_change='fail',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
        partitioned_by=['portal_run_date']
    )
}}

with transformed as (

    select
        wfl.portal_run_id,
        cast(parse_datetime(substr(portal_run_id, 1, 8), 'yyyymmdd') as date) as portal_run_date,
        lib.library_id,
        sat.breakendid5,
        sat.breakendid3,
        sat."name",
        sat.reported,
        sat.reported_type,
        sat.reportable_reasons,
        sat.phased,
        sat.likelihood,
        sat.chain_length,
        sat.chain_links,
        sat.chain_terminated,
        sat.domains_kept,
        sat.domains_lost,
        sat.skipped_exons_up,
        sat.skipped_exons_down,
        sat.fused_exon_up,
        sat.fused_exon_down,
        sat.gene_start,
        sat.gene_context_start,
        sat.transcript_start,
        sat.gene_end,
        sat.gene_context_end,
        sat.transcript_end,
        sat.junction_cn
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_linx_fusions') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

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
