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
        sat.cdr3_seq,
        sat.cdr3_aa,
        sat.locus,
        sat.filter,
        sat.blastn_status,
        sat.min_high_qual_base_reads,
        sat.assigned_reads,
        sat.v_aligned_reads,
        sat.j_aligned_reads,
        sat.in_frame,
        sat.contains_stop,
        sat.v_type,
        sat.v_anchor_start,
        sat.v_anchor_end,
        sat.v_anchor_seq,
        sat.v_anchor_template_seq,
        sat.v_anchor_aa,
        sat.v_anchor_template_aa,
        sat.v_match_method,
        sat.v_similarity_score,
        sat.v_non_split_reads,
        sat.j_type,
        sat.j_anchor_start,
        sat.j_anchor_end,
        sat.j_anchor_seq,
        sat.j_anchor_template_seq,
        sat.j_anchor_aa,
        sat.j_anchor_template_aa,
        sat.j_match_method,
        sat.j_similarity_score,
        sat.j_non_split_reads,
        sat.v_gene,
        sat.v_pident,
        sat.v_align_start,
        sat.v_align_end,
        sat.d_gene,
        sat.d_pident,
        sat.d_align_start,
        sat.d_align_end,
        sat.j_gene,
        sat.j_pident,
        sat.j_align_start,
        sat.j_align_end,
        sat.v_primer_matches,
        sat.j_primer_matches,
        sat.layout_id,
        sat.full_seq,
        sat.support
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_cider_vdj') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(cdr3_seq as varchar) as cdr3_seq,
        cast(cdr3_aa as varchar) as cdr3_aa,
        cast(locus as varchar) as locus,
        cast(filter as varchar) as filter,
        cast(blastn_status as varchar) as blastn_status,
        cast(min_high_qual_base_reads as double) as min_high_qual_base_reads,
        cast(assigned_reads as double) as assigned_reads,
        cast(v_aligned_reads as double) as v_aligned_reads,
        cast(j_aligned_reads as double) as j_aligned_reads,
        cast(in_frame as varchar) as in_frame,
        cast(contains_stop as varchar) as contains_stop,
        cast(v_type as varchar) as v_type,
        cast(v_anchor_start as double) as v_anchor_start,
        cast(v_anchor_end as double) as v_anchor_end,
        cast(v_anchor_seq as varchar) as v_anchor_seq,
        cast(v_anchor_template_seq as varchar) as v_anchor_template_seq,
        cast(v_anchor_aa as varchar) as v_anchor_aa,
        cast(v_anchor_template_aa as varchar) as v_anchor_template_aa,
        cast(v_match_method as varchar) as v_match_method,
        cast(v_similarity_score as double) as v_similarity_score,
        cast(v_non_split_reads as double) as v_non_split_reads,
        cast(j_type as varchar) as j_type,
        cast(j_anchor_start as double) as j_anchor_start,
        cast(j_anchor_end as double) as j_anchor_end,
        cast(j_anchor_seq as varchar) as j_anchor_seq,
        cast(j_anchor_template_seq as varchar) as j_anchor_template_seq,
        cast(j_anchor_aa as varchar) as j_anchor_aa,
        cast(j_anchor_template_aa as varchar) as j_anchor_template_aa,
        cast(j_match_method as varchar) as j_match_method,
        cast(j_similarity_score as double) as j_similarity_score,
        cast(j_non_split_reads as double) as j_non_split_reads,
        cast(v_gene as varchar) as v_gene,
        cast(v_pident as double) as v_pident,
        cast(v_align_start as double) as v_align_start,
        cast(v_align_end as double) as v_align_end,
        cast(d_gene as varchar) as d_gene,
        cast(d_pident as double) as d_pident,
        cast(d_align_start as double) as d_align_start,
        cast(d_align_end as double) as d_align_end,
        cast(j_gene as varchar) as j_gene,
        cast(j_pident as double) as j_pident,
        cast(j_align_start as double) as j_align_start,
        cast(j_align_end as double) as j_align_end,
        cast(v_primer_matches as double) as v_primer_matches,
        cast(j_primer_matches as double) as j_primer_matches,
        cast(layout_id as varchar) as layout_id,
        cast(full_seq as varchar) as full_seq,
        cast(support as varchar) as support
    from
        transformed

)

select * from final
