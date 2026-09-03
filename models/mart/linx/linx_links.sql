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
        sat.chain_index,
        sat.chain_count,
        sat.lower_sv_id,
        sat.upper_sv_id,
        sat.lower_breakend_is_start,
        sat.upper_breakend_is_start,
        sat.chrom,
        sat.arm,
        sat.assembled,
        sat.traversed_sv_count,
        sat."length",
        sat.junction_cn,
        sat.junction_cn_uncertainty,
        sat.pseudogene_info,
        sat.ecdna
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_linx_links') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(cluster_id as varchar) as cluster_id,
        cast(chain_id as varchar) as chain_id,
        cast(chain_index as varchar) as chain_index,
        cast(chain_count as double) as chain_count,
        cast(lower_sv_id as varchar) as lower_sv_id,
        cast(upper_sv_id as varchar) as upper_sv_id,
        cast(lower_breakend_is_start as varchar) as lower_breakend_is_start,
        cast(upper_breakend_is_start as varchar) as upper_breakend_is_start,
        cast(chrom as varchar) as chrom,
        cast(arm as varchar) as arm,
        cast(assembled as varchar) as assembled,
        cast(traversed_sv_count as double) as traversed_sv_count,
        cast("length" as double) as "length",
        cast(junction_cn as double) as junction_cn,
        cast(junction_cn_uncertainty as double) as junction_cn_uncertainty,
        cast(pseudogene_info as varchar) as pseudogene_info,
        cast(ecdna as varchar) as ecdna
    from
        transformed

)

select * from final
