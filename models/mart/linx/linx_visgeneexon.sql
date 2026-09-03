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
        sat.gene,
        sat.transcript,
        sat.chrom,
        sat.annotation_type,
        sat.exon_rank,
        sat.exon_start,
        sat.exon_end
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_linx_visgeneexon') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(cluster_id as varchar) as cluster_id,
        cast(gene as varchar) as gene,
        cast(transcript as varchar) as transcript,
        cast(chrom as varchar) as chrom,
        cast(annotation_type as varchar) as annotation_type,
        cast(exon_rank as varchar) as exon_rank,
        cast(exon_start as varchar) as exon_start,
        cast(exon_end as varchar) as exon_end
    from
        transformed

)

select * from final
