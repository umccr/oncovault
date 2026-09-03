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
        sat."type",
        sat.tel_length_raw,
        sat.tel_length_final,
        sat.fragments_full,
        sat.fragments_c_rich_partial,
        sat.fragments_g_rich_partial,
        sat.reads_telomeric_total,
        sat.purity,
        sat.ploidy,
        sat.dup_prop,
        sat.dp_read_mean,
        sat.dp_read_gc50
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_teal_tellength') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(sample_id as varchar) as sample_id,
        cast("type" as varchar) as "type",
        cast(tel_length_raw as double) as tel_length_raw,
        cast(tel_length_final as double) as tel_length_final,
        cast(fragments_full as double) as fragments_full,
        cast(fragments_c_rich_partial as double) as fragments_c_rich_partial,
        cast(fragments_g_rich_partial as double) as fragments_g_rich_partial,
        cast(reads_telomeric_total as double) as reads_telomeric_total,
        cast(purity as double) as purity,
        cast(ploidy as double) as ploidy,
        cast(dup_prop as double) as dup_prop,
        cast(dp_read_mean as double) as dp_read_mean,
        cast(dp_read_gc50 as double) as dp_read_gc50
    from
        transformed

)

select * from final

