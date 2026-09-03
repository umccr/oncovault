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
        sat.gene,
        sat.cn_min,
        sat.cn_max,
        sat.somatic_regions,
        sat.transcript_id,
        sat.is_canonical,
        sat.chrom_band,
        sat.regions_min,
        sat.start_region_min,
        sat.end_region_min,
        sat.start_region_min_support,
        sat.end_region_min_support,
        sat.region_min_method,
        sat.cn_minor_allele_min,
        sat.window_count_depth,
        sat.gc_content
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_purple_cnvgenetsv') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(chrom as varchar) as chrom,
        cast("start" as double) as "start",
        cast("end" as double) as "end",
        cast(gene as varchar) as gene,
        cast(cn_min as double) as cn_min,
        cast(cn_max as double) as cn_max,
        cast(somatic_regions as double) as somatic_regions,
        cast(transcript_id as varchar) as transcript_id,
        cast(is_canonical as varchar) as is_canonical,
        cast(chrom_band as varchar) as chrom_band,
        cast(regions_min as double) as regions_min,
        cast(start_region_min as double) as start_region_min,
        cast(end_region_min as double) as end_region_min,
        cast(start_region_min_support as varchar) as start_region_min_support,
        cast(end_region_min_support as varchar) as end_region_min_support,
        cast(region_min_method as varchar) as region_min_method,
        cast(cn_minor_allele_min as double) as cn_minor_allele_min,
        cast(window_count_depth as double) as window_count_depth,
        cast(gc_content as double) as gc_content
    from
        transformed

)

select * from final
