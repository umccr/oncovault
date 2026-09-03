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
        sat.qc_status,
        sat.method,
        sat.cn_segments,
        sat.cn_segments_unsupported,
        sat.purity,
        sat.gender_amber,
        sat.gender_cobalt,
        sat.deleted_genes,
        sat.contamination,
        sat.germline_aberrations,
        sat.mean_depth_amber,
        sat.loh_percent,
        sat.tinc_level,
        sat.chimerism_percent
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_purple_qc') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(qc_status as varchar) as qc_status,
        cast("method" as varchar) as "method",
        cast(cn_segments as bigint) as cn_segments,
        cast(cn_segments_unsupported as bigint) as cn_segments_unsupported,
        cast(purity as double) as purity,
        cast(gender_amber as varchar) as gender_amber,
        cast(gender_cobalt as varchar) as gender_cobalt,
        cast(deleted_genes as bigint) as deleted_genes,
        cast(contamination as double) as contamination,
        cast(germline_aberrations as varchar) as germline_aberrations,
        cast(mean_depth_amber as double) as mean_depth_amber,
        cast(loh_percent as double) as loh_percent,
        cast(tinc_level as double) as tinc_level,
        cast(chimerism_percent as double) as chimerism_percent
    from
        transformed

)

select * from final
