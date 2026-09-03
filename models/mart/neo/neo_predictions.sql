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
        sat.ne_id,
        sat.variant_type,
        sat.variant_info,
        sat.gene_name,
        sat.aa_up,
        sat.aa_novel,
        sat.aa_down,
        sat.peptide_count,
        sat.tpm_source,
        sat.rna_frags,
        sat.rna_depth,
        sat.tpm_up,
        sat.tpm_down,
        sat.tpm_expected,
        sat.tpm_raw_effective,
        sat.tpm_effective,
        sat.tpm_cancer_up,
        sat.tpm_cancer_down,
        sat.tpm_pancancer_up,
        sat.tpm_pancancer_down,
        sat.nmd_min,
        sat.nmd_max,
        sat.coding_bases_length_min,
        sat.coding_bases_length_max,
        sat.fused_intron_length,
        sat.skipped_donors,
        sat.skipped_acceptors,
        sat.transcripts_up,
        sat.transcripts_down,
        sat.variant_cn,
        sat.cn,
        sat.subclonal_likelihood
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_neo_predictions') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(ne_id as bigint) as ne_id,
        cast(variant_type as varchar) as variant_type,
        cast(variant_info as varchar) as variant_info,
        cast(gene_name as varchar) as gene_name,
        cast(aa_up as varchar) as aa_up,
        cast(aa_novel as varchar) as aa_novel,
        cast(aa_down as varchar) as aa_down,
        cast(peptide_count as double) as peptide_count,
        cast(tpm_source as varchar) as tpm_source,
        cast(rna_frags as double) as rna_frags,
        cast(rna_depth as double) as rna_depth,
        cast(tpm_up as double) as tpm_up,
        cast(tpm_down as double) as tpm_down,
        cast(tpm_expected as double) as tpm_expected,
        cast(tpm_raw_effective as double) as tpm_raw_effective,
        cast(tpm_effective as double) as tpm_effective,
        cast(tpm_cancer_up as double) as tpm_cancer_up,
        cast(tpm_cancer_down as double) as tpm_cancer_down,
        cast(tpm_pancancer_up as double) as tpm_pancancer_up,
        cast(tpm_pancancer_down as double) as tpm_pancancer_down,
        cast(nmd_min as double) as nmd_min,
        cast(nmd_max as double) as nmd_max,
        cast(coding_bases_length_min as double) as coding_bases_length_min,
        cast(coding_bases_length_max as double) as coding_bases_length_max,
        cast(fused_intron_length as double) as fused_intron_length,
        cast(skipped_donors as double) as skipped_donors,
        cast(skipped_acceptors as double) as skipped_acceptors,
        cast(transcripts_up as varchar) as transcripts_up,
        cast(transcripts_down as varchar) as transcripts_down,
        cast(variant_cn as double) as variant_cn,
        cast(cn as double) as cn,
        cast(subclonal_likelihood as double) as subclonal_likelihood
    from
        transformed

)

select * from final

