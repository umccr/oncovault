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
        sat.sample_id,
        sat.chrom,
        sat.arm,
        sat.start_pos,
        sat.end_pos,
        sat.n_probes,
        sat.mean
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_cobalt_ratiopcf') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(sample_id as varchar) as sample_id,
        cast(chrom as varchar) as chrom,
        cast(arm as varchar) as arm,
        cast(start_pos as double) as start_pos,
        cast(end_pos as double) as end_pos,
        cast(n_probes as bigint) as n_probes,
        cast(mean as double) as mean
    from
        transformed

)

select * from final

