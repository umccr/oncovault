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
        sat.chrom,
        sat."position",
        sat.orientation,
        sat.junc_frags,
        sat.exact_support_frags,
        sat.other_support_frags,
        sat.low_map_qual_frags,
        sat.max_qual,
        sat.extra_info,
        sat.indel,
        sat.hotspot,
        sat.initial_read_id
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_esvee_prepjunction') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(chrom as varchar) as chrom,
        cast("position" as double) as "position",
        cast(orientation as double) as orientation,
        cast(junc_frags as double) as junc_frags,
        cast(exact_support_frags as double) as exact_support_frags,
        cast(other_support_frags as double) as other_support_frags,
        cast(low_map_qual_frags as double) as low_map_qual_frags,
        cast(max_qual as double) as max_qual,
        cast(extra_info as double) as extra_info,
        cast(indel as varchar) as indel,
        cast(hotspot as varchar) as hotspot,
        cast(initial_read_id as varchar) as initial_read_id
    from
        transformed

)

select * from final

