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
        sat.pos,
        sat."ref",
        sat.alt,
        sat.dp_normal,
        sat.refsup_normal,
        sat.altsup_normal,
        sat.dp_tumor,
        sat.refsup_tumor,
        sat.altsup_tumor
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_amber_contaminationtsv') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(chrom as varchar) as chrom,
        cast(pos as bigint) as pos,
        cast("ref" as varchar) as "ref",
        cast(alt as varchar) as alt,
        cast(dp_normal as bigint) as dp_normal,
        cast(refsup_normal as bigint) as refsup_normal,
        cast(altsup_normal as bigint) as altsup_normal,
        cast(dp_tumor as bigint) as dp_tumor,
        cast(refsup_tumor as bigint) as refsup_tumor,
        cast(altsup_tumor as bigint) as altsup_tumor
    from
        transformed

)

select * from final
