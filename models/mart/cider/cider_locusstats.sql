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
        sat.locus,
        sat.reads_used,
        sat.reads_total,
        sat.downsampled,
        sat.sequences,
        sat.sequences_pass
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_cider_locusstats') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(locus as varchar) as locus,
        cast(reads_used as double) as reads_used,
        cast(reads_total as double) as reads_total,
        cast(downsampled as varchar) as downsampled,
        cast(sequences as double) as sequences,
        cast(sequences_pass as double) as sequences_pass
    from
        transformed

)

select * from final
