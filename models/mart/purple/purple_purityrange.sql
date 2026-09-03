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
        sat.purity,
        sat.norm_factor,
        sat.score,
        sat.diploid_proportion,
        sat.ploidy,
        sat.somatic_penalty
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_purple_purityrange') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(purity as double) as purity,
        cast(norm_factor as double) as norm_factor,
        cast(score as double) as score,
        cast(diploid_proportion as double) as diploid_proportion,
        cast(ploidy as double) as ploidy,
        cast(somatic_penalty as double) as somatic_penalty
    from
        transformed

)

select * from final
