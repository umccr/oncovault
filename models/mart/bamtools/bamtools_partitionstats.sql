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
        sat.pos_start,
        sat.pos_end,
        sat.total_reads,
        sat.dup_reads,
        sat.chim_reads,
        sat.interpartition,
        sat.unmap_reads,
        sat.process_time
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_bamtools_partitionstats') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(chrom as varchar) as chrom,
        cast(pos_start as double) as pos_start,
        cast(pos_end as double) as pos_end,
        cast(total_reads as double) as total_reads,
        cast(dup_reads as double) as dup_reads,
        cast(chim_reads as double) as chim_reads,
        cast(interpartition as double) as interpartition,
        cast(unmap_reads as double) as unmap_reads,
        cast(process_time as double) as process_time
    from
        transformed

)

select * from final
