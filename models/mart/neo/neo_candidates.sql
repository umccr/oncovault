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
        sat.variant_cn,
        sat.cn,
        sat.subclonal_likelihood,
        sat.gene_id_up,
        sat.gene_id_down,
        sat.gene_name_up,
        sat.gene_name_down,
        sat.chr_up,
        sat.chr_down,
        sat.orient_up,
        sat.orient_down,
        sat.aa_upstream,
        sat.aa_downstream,
        sat.aa_novel,
        sat.nmd_min,
        sat.nmd_max,
        sat.coding_bases_length_min,
        sat.coding_bases_length_max,
        sat.fused_intron_length,
        sat.skipped_donors,
        sat.skipped_acceptors,
        sat.transcripts_up,
        sat.transcripts_down,
        sat.aa_wildtype,
        sat.coding_base_up_pos_start,
        sat.coding_base_up_pos_end,
        sat.coding_bases_up,
        sat.coding_base_cigar_up,
        sat.coding_base_down_pos_start,
        sat.coding_base_down_pos_end,
        sat.coding_bases_down,
        sat.coding_base_cigar_down
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_neo_candidates') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(ne_id as bigint) as ne_id,
        cast(variant_type as varchar) as variant_type,
        cast(variant_info as varchar) as variant_info,
        cast(variant_cn as double) as variant_cn,
        cast(cn as double) as cn,
        cast(subclonal_likelihood as double) as subclonal_likelihood,
        cast(gene_id_up as varchar) as gene_id_up,
        cast(gene_id_down as varchar) as gene_id_down,
        cast(gene_name_up as varchar) as gene_name_up,
        cast(gene_name_down as varchar) as gene_name_down,
        cast(chr_up as varchar) as chr_up,
        cast(chr_down as varchar) as chr_down,
        cast(orient_up as double) as orient_up,
        cast(orient_down as double) as orient_down,
        cast(aa_upstream as varchar) as aa_upstream,
        cast(aa_downstream as varchar) as aa_downstream,
        cast(aa_novel as varchar) as aa_novel,
        cast(nmd_min as double) as nmd_min,
        cast(nmd_max as double) as nmd_max,
        cast(coding_bases_length_min as double) as coding_bases_length_min,
        cast(coding_bases_length_max as double) as coding_bases_length_max,
        cast(fused_intron_length as double) as fused_intron_length,
        cast(skipped_donors as double) as skipped_donors,
        cast(skipped_acceptors as double) as skipped_acceptors,
        cast(transcripts_up as varchar) as transcripts_up,
        cast(transcripts_down as varchar) as transcripts_down,
        cast(aa_wildtype as varchar) as aa_wildtype,
        cast(coding_base_up_pos_start as double) as coding_base_up_pos_start,
        cast(coding_base_up_pos_end as double) as coding_base_up_pos_end,
        cast(coding_bases_up as varchar) as coding_bases_up,
        cast(coding_base_cigar_up as varchar) as coding_base_cigar_up,
        cast(coding_base_down_pos_start as double) as coding_base_down_pos_start,
        cast(coding_base_down_pos_end as double) as coding_base_down_pos_end,
        cast(coding_bases_down as varchar) as coding_bases_down,
        cast(coding_base_cigar_down as varchar) as coding_base_cigar_down
    from
        transformed

)

select * from final

