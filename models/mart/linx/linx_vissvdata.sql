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
        sat.sv_id,
        sat."type",
        sat.resolved_type,
        sat.is_synthetic,
        sat.chr_start,
        sat.chr_end,
        sat.pos_start,
        sat.pos_end,
        sat.orient_start,
        sat.orient_end,
        sat.info_start,
        sat.info_end,
        sat.junction_cn,
        sat.in_double_minute
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_linx_vissvdata') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(cluster_id as varchar) as cluster_id,
        cast(chain_id as varchar) as chain_id,
        cast(sv_id as varchar) as sv_id,
        cast("type" as varchar) as "type",
        cast(resolved_type as varchar) as resolved_type,
        cast(is_synthetic as varchar) as is_synthetic,
        cast(chr_start as varchar) as chr_start,
        cast(chr_end as varchar) as chr_end,
        cast(pos_start as double) as pos_start,
        cast(pos_end as double) as pos_end,
        cast(orient_start as double) as orient_start,
        cast(orient_end as double) as orient_end,
        cast(info_start as varchar) as info_start,
        cast(info_end as varchar) as info_end,
        cast(junction_cn as double) as junction_cn,
        cast(in_double_minute as varchar) as in_double_minute
    from
        transformed

)

select * from final
