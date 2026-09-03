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
        sat.sample_id,
        sat.clf_group,
        sat.clf_name,
        sat.rank,
        sat.class,
        sat.prob,
        sat.extra_info,
        sat.extra_info_format
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_cuppa_predsum') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(sample_id as varchar) as sample_id,
        cast(clf_group as varchar) as clf_group,
        cast(clf_name as varchar) as clf_name,
        cast(rank as bigint) as rank,
        cast(class as varchar) as class,
        cast(prob as double) as prob,
        cast(extra_info as varchar) as extra_info,
        cast(extra_info_format as varchar) as extra_info_format
    from
        transformed

)

select * from final

