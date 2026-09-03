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
        sat.tot_reads,
        sat.prep_reads,
        sat.translocation,
        sat.inv_lt_1k,
        sat.inv_1_to_5k,
        sat.inv_5_to_100k,
        sat.inv_gt_100k,
        sat.del_1_to_5k,
        sat.del_5_to_100k,
        sat.del_gt_100k,
        sat.dup_1_to_5k,
        sat.dup_5_to_100k,
        sat.dup_gt_100k
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_esvee_prepdiscstats') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(tot_reads as double) as tot_reads,
        cast(prep_reads as double) as prep_reads,
        cast(translocation as double) as translocation,
        cast(inv_lt_1k as double) as inv_lt_1k,
        cast(inv_1_to_5k as double) as inv_1_to_5k,
        cast(inv_5_to_100k as double) as inv_5_to_100k,
        cast(inv_gt_100k as double) as inv_gt_100k,
        cast(del_1_to_5k as double) as del_1_to_5k,
        cast(del_5_to_100k as double) as del_5_to_100k,
        cast(del_gt_100k as double) as del_gt_100k,
        cast(dup_1_to_5k as double) as dup_1_to_5k,
        cast(dup_5_to_100k as double) as dup_5_to_100k,
        cast(dup_gt_100k as double) as dup_gt_100k
    from
        transformed

)

select * from final

