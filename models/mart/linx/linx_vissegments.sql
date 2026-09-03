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
        sat.cluster_id,
        sat.chain_id,
        sat.chrom,
        sat.pos_start,
        sat.pos_end,
        sat.link_ploidy,
        sat.in_double_minute
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_linx_vissegments') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(cluster_id as varchar) as cluster_id,
        cast(chain_id as varchar) as chain_id,
        cast(chrom as varchar) as chrom,
        cast(pos_start as varchar) as pos_start,
        cast(pos_end as varchar) as pos_end,
        cast(link_ploidy as double) as link_ploidy,
        cast(in_double_minute as varchar) as in_double_minute
    from
        transformed

)

select * from final
