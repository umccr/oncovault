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
        sat.passed_or_failed,
        sat.total,
        sat."primary",
        sat.secondary,
        sat.suppl,
        sat.dup,
        sat.primary_dup,
        sat.mapped,
        sat.primary_map,
        sat.paired_in_seq,
        sat.read1,
        sat.read2,
        sat.proper_pair,
        sat.both_map,
        sat.singletons,
        sat.matemap_diff,
        sat.matemap_diff_mapq5,
        sat.mapped_pct,
        sat.primary_map_pct,
        sat.proper_pair_pct,
        sat.singletons_pct
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_bamtools_flagstats') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(passed_or_failed as varchar) as passed_or_failed,
        cast(total as double) as total,
        cast("primary" as double) as "primary",
        cast(secondary as double) as secondary,
        cast(suppl as double) as suppl,
        cast(dup as double) as dup,
        cast(primary_dup as double) as primary_dup,
        cast(mapped as double) as mapped,
        cast(primary_map as double) as primary_map,
        cast(paired_in_seq as double) as paired_in_seq,
        cast(read1 as double) as read1,
        cast(read2 as double) as read2,
        cast(proper_pair as double) as proper_pair,
        cast(both_map as double) as both_map,
        cast(singletons as double) as singletons,
        cast(matemap_diff as double) as matemap_diff,
        cast(matemap_diff_mapq5 as double) as matemap_diff_mapq5,
        cast(mapped_pct as double) as mapped_pct,
        cast(primary_map_pct as double) as primary_map_pct,
        cast(proper_pair_pct as double) as proper_pair_pct,
        cast(singletons_pct as double) as singletons_pct
    from
        transformed

)

select * from final
