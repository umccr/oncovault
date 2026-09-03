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
        sat.batch_id,
        sat.batch_date,
        sat.batch_file_count,
        sat.batch_table_count,
        sat.pkg_name,
        sat.pkg_version
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('sat_tidywigits_metadata') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

pivoted as (

    select
        portal_run_id,
        portal_run_date,
        batch_id,
        batch_date,
        batch_file_count,
        batch_table_count,
        max(case when pkg_name = 'tidywigits' then pkg_version end) as tidywigits_version,
        max(case when pkg_name = 'nemo' then pkg_version end) as nemo_version
    from transformed
    group by
        portal_run_id,
        portal_run_date,
        batch_id,
        batch_date,
        batch_file_count,
        batch_table_count

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,

        cast(batch_id as varchar(26)) as batch_id,
        cast(batch_date as date) as batch_date,
        cast(batch_file_count as bigint) as batch_file_count,
        cast(batch_table_count as bigint) as batch_table_count,
        cast(tidywigits_version as varchar(255)) as tidywigits_version,
        cast(nemo_version as varchar(255)) as nemo_version
    from
        pivoted

)

select * from final
