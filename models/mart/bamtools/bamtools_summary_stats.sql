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
        sat.tot_region_bases,
        sat.tot_reads,
        sat.dup_reads,
        sat.dual_strand_reads,
        sat.cov_mean,
        sat.cov_sd,
        sat.cov_median,
        sat.cov_mad,
        sat.lowmapq_pct,
        sat.dup_pct,
        sat.unpaired_pct,
        sat.lowbaseq_pct,
        sat.overlap_read_pct,
        sat.cov_capped
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_bamtools_summary_stats') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

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
