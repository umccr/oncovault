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
        sat.vcf_id,
        sat.sv_id,
        sat.cluster_id,
        sat.cluster_reason,
        sat.fragile_site_start,
        sat.fragile_site_end,
        sat.is_foldback,
        sat.linetype_start,
        sat.linetype_end,
        sat.junction_cn_min,
        sat.junction_cn_max,
        sat.gene_start,
        sat.gene_end,
        sat.local_topology_id_start,
        sat.local_topology_id_end,
        sat.local_topology_start,
        sat.local_topology_end,
        sat.local_ti_count_start,
        sat.local_ti_count_end
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_linx_svs') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(vcf_id as varchar) as vcf_id,
        cast(sv_id as varchar) as sv_id,
        cast(cluster_id as varchar) as cluster_id,
        cast(cluster_reason as varchar) as cluster_reason,
        cast(fragile_site_start as varchar) as fragile_site_start,
        cast(fragile_site_end as varchar) as fragile_site_end,
        cast(is_foldback as varchar) as is_foldback,
        cast(linetype_start as varchar) as linetype_start,
        cast(linetype_end as varchar) as linetype_end,
        cast(junction_cn_min as double) as junction_cn_min,
        cast(junction_cn_max as double) as junction_cn_max,
        cast(gene_start as varchar) as gene_start,
        cast(gene_end as varchar) as gene_end,
        cast(local_topology_id_start as varchar) as local_topology_id_start,
        cast(local_topology_id_end as varchar) as local_topology_id_end,
        cast(local_topology_start as varchar) as local_topology_start,
        cast(local_topology_end as varchar) as local_topology_end,
        cast(local_ti_count_start as double) as local_ti_count_start,
        cast(local_ti_count_end as double) as local_ti_count_end
    from
        transformed

)

select * from final
