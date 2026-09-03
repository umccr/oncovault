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
        sat.match_type,
        sat.gene,
        sat.functionality,
        sat.p_ident,
        sat.seq_length,
        sat.align_start,
        sat.align_end,
        sat.align_gaps,
        sat.align_evalue,
        sat.align_bitscore,
        sat.ref_strand,
        sat.ref_start,
        sat.ref_end,
        sat.ref_contig,
        sat.ref_seq
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_cider_blastn') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(cdr3_seq as varchar) as cdr3_seq,
        cast(cdr3_aa as varchar) as cdr3_aa,
        cast(match_type as varchar) as match_type,
        cast(gene as varchar) as gene,
        cast(functionality as varchar) as functionality,
        cast(p_ident as double) as p_ident,
        cast(seq_length as double) as seq_length,
        cast(align_start as double) as align_start,
        cast(align_end as double) as align_end,
        cast(align_gaps as double) as align_gaps,
        cast(align_evalue as double) as align_evalue,
        cast(align_bitscore as double) as align_bitscore,
        cast(ref_strand as varchar) as ref_strand,
        cast(ref_start as double) as ref_start,
        cast(ref_end as double) as ref_end,
        cast(ref_contig as varchar) as ref_contig,
        cast(ref_seq as varchar) as ref_seq
    from
        transformed

)

select * from final
