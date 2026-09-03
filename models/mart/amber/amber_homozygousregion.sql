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
        sat.n_snp,
        sat.n_hom,
        sat.n_het,
        sat.filter
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_amber_homozygousregion') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(chrom as varchar) as chrom,
        cast(pos_start as bigint) as pos_start,
        cast(pos_end as bigint) as pos_end,
        cast(n_snp as bigint) as n_snp,
        cast(n_hom as bigint) as n_hom,
        cast(n_het as bigint) as n_het,
        cast(filter as varchar) as filter
    from
        transformed

)

select * from final
