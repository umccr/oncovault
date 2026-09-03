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
        sat.chrom_band,
        sat.gene,
        sat.transcript,
        sat.is_canonical,
        sat.driver,
        sat.category,
        sat.likelihood_method,
        sat.driver_likelihood,
        sat.missense,
        sat.nonsense,
        sat.splice,
        sat.inframe,
        sat.frameshift,
        sat.biallelic,
        sat.min_cn,
        sat.max_cn
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_linx_drivercatalog') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(chrom as varchar) as chrom,
        cast(chrom_band as varchar) as chrom_band,
        cast(gene as varchar) as gene,
        cast(transcript as varchar) as transcript,
        cast(is_canonical as varchar) as is_canonical,
        cast(driver as varchar) as driver,
        cast(category as varchar) as category,
        cast(likelihood_method as varchar) as likelihood_method,
        cast(driver_likelihood as double) as driver_likelihood,
        cast(missense as double) as missense,
        cast(nonsense as double) as nonsense,
        cast(splice as double) as splice,
        cast(inframe as double) as inframe,
        cast(frameshift as double) as frameshift,
        cast(biallelic as varchar) as biallelic,
        cast(min_cn as double) as min_cn,
        cast(max_cn as double) as max_cn
    from
        transformed

)

select * from final
