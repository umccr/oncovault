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
        sat."start",
        sat."end",
        sat.cn,
        sat.baf_count,
        sat.baf_observed,
        sat.baf,
        sat.start_segment_support,
        sat.end_segment_support,
        sat."method",
        sat.window_count_depth,
        sat.gc_content,
        sat.start_min,
        sat.start_max,
        sat.cn_minor_allele,
        sat.cn_major_allele
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_purple_cnvsomtsv') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(chrom as varchar) as chrom,
        cast("start" as double) as "start",
        cast("end" as double) as "end",
        cast(cn as double) as cn,
        cast(baf_count as double) as baf_count,
        cast(baf_observed as double) as baf_observed,
        cast(baf as double) as baf,
        cast(start_segment_support as varchar) as start_segment_support,
        cast(end_segment_support as varchar) as end_segment_support,
        cast("method" as varchar) as "method",
        cast(window_count_depth as double) as window_count_depth,
        cast(gc_content as double) as gc_content,
        cast(start_min as double) as start_min,
        cast(start_max as double) as start_max,
        cast(cn_minor_allele as double) as cn_minor_allele,
        cast(cn_major_allele as double) as cn_major_allele
    from
        transformed

)

select * from final
