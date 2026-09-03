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
        sat.allele,
        sat.ref_total,
        sat.ref_unique,
        sat.ref_shared,
        sat.ref_wild,
        sat.tumor_total,
        sat.tumor_unique,
        sat.tumor_shared,
        sat.tumor_wild,
        sat.rna_total,
        sat.rna_unique,
        sat.rna_shared,
        sat.rna_wild,
        sat.tumor_cn,
        sat.somatic_missense,
        sat.somatic_nonsense_or_frameshift,
        sat.somatic_splice,
        sat.somatic_synonymous,
        sat.somatic_inframe_indel
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_lilac_summary') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(allele as varchar) as allele,
        cast(ref_total as double) as ref_total,
        cast(ref_unique as double) as ref_unique,
        cast(ref_shared as double) as ref_shared,
        cast(ref_wild as double) as ref_wild,
        cast(tumor_total as double) as tumor_total,
        cast(tumor_unique as double) as tumor_unique,
        cast(tumor_shared as double) as tumor_shared,
        cast(tumor_wild as double) as tumor_wild,
        cast(rna_total as double) as rna_total,
        cast(rna_unique as double) as rna_unique,
        cast(rna_shared as double) as rna_shared,
        cast(rna_wild as double) as rna_wild,
        cast(tumor_cn as double) as tumor_cn,
        cast(somatic_missense as double) as somatic_missense,
        cast(somatic_nonsense_or_frameshift as double) as somatic_nonsense_or_frameshift,
        cast(somatic_splice as double) as somatic_splice,
        cast(somatic_synonymous as double) as somatic_synonymous,
        cast(somatic_inframe_indel as double) as somatic_inframe_indel
    from
        transformed

)

select * from final

