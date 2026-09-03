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
        sat.gene,
        sat.chrom,
        sat.chrom_band,
        sat.start_region,
        sat.end_region,
        sat.window_count_depth,
        sat.start_exon,
        sat.end_exon,
        sat.detection_method,
        sat.status_germline,
        sat.status_tumor,
        sat.cn_germline,
        sat.cn_tumor,
        sat.filter,
        sat.cohort_frequency,
        sat.reported
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_purple_germdeltsv') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(gene as varchar) as gene,
        cast(chrom as varchar) as chrom,
        cast(chrom_band as varchar) as chrom_band,
        cast(start_region as double) as start_region,
        cast(end_region as double) as end_region,
        cast(window_count_depth as double) as window_count_depth,
        cast(start_exon as double) as start_exon,
        cast(end_exon as double) as end_exon,
        cast(detection_method as varchar) as detection_method,
        cast(status_germline as varchar) as status_germline,
        cast(status_tumor as varchar) as status_tumor,
        cast(cn_germline as double) as cn_germline,
        cast(cn_tumor as double) as cn_tumor,
        cast(filter as varchar) as filter,
        cast(cohort_frequency as double) as cohort_frequency,
        cast(reported as varchar) as reported
    from
        transformed

)

select * from final
