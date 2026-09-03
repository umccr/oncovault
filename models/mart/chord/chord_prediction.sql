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
        sat.p_brca1,
        sat.p_brca2,
        sat.p_hrd,
        sat.hr_status,
        sat.hrd_type,
        sat.remarks_hr_status,
        sat.remarks_hrd_type
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_chord_prediction') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(sample_id as varchar) as sample_id,
        cast(p_brca1 as double) as p_brca1,
        cast(p_brca2 as double) as p_brca2,
        cast(p_hrd as double) as p_hrd,
        cast(hr_status as varchar) as hr_status,
        cast(hrd_type as varchar) as hrd_type,
        cast(remarks_hr_status as varchar) as remarks_hr_status,
        cast(remarks_hrd_type as varchar) as remarks_hrd_type
    from
        transformed

)

select * from final
