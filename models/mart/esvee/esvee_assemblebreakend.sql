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
        sat.id,
        sat.phase_group_id,
        sat.phase_set_id,
        sat.assembly_id,
        sat.mate_id,
        sat.assembly_info,
        sat."type",
        sat.chrom,
        sat."position",
        sat.orientation,
        sat.mate_chr,
        sat.mate_pos,
        sat.mate_orient,
        sat."length",
        sat.inserted_bases,
        sat.homology,
        sat.confidence_interval,
        sat.inexact_offset,
        sat.qual,
        sat.split_fragments,
        sat.ref_split_fragments,
        sat.disc_fragments,
        sat.ref_disc_fragments,
        sat.forward_reads,
        sat.reverse_reads,
        sat.sequence_length,
        sat.segment_count,
        sat.segment_index,
        sat.sequence_index,
        sat.aligned_bases,
        sat.map_qual,
        sat.score,
        sat.adj_aligned_bases,
        sat.avg_fragment_length,
        sat.incomplete_fragments,
        sat.breakend_qual,
        sat.facing_breakend_ids,
        sat.alt_alignments,
        sat.insertion_type,
        sat.unique_frag_pos,
        sat.closest_assembly,
        sat.non_primary_frags
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_esvee_assemblebreakend') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(id as varchar) as id,
        cast(phase_group_id as varchar) as phase_group_id,
        cast(phase_set_id as varchar) as phase_set_id,
        cast(assembly_id as varchar) as assembly_id,
        cast(mate_id as varchar) as mate_id,
        cast(assembly_info as varchar) as assembly_info,
        cast("type" as varchar) as "type",
        cast(chrom as varchar) as chrom,
        cast("position" as double) as "position",
        cast(orientation as double) as orientation,
        cast(mate_chr as varchar) as mate_chr,
        cast(mate_pos as double) as mate_pos,
        cast(mate_orient as double) as mate_orient,
        cast("length" as double) as "length",
        cast(inserted_bases as varchar) as inserted_bases,
        cast(homology as varchar) as homology,
        cast(confidence_interval as varchar) as confidence_interval,
        cast(inexact_offset as varchar) as inexact_offset,
        cast(qual as double) as qual,
        cast(split_fragments as double) as split_fragments,
        cast(ref_split_fragments as double) as ref_split_fragments,
        cast(disc_fragments as double) as disc_fragments,
        cast(ref_disc_fragments as double) as ref_disc_fragments,
        cast(forward_reads as double) as forward_reads,
        cast(reverse_reads as double) as reverse_reads,
        cast(sequence_length as double) as sequence_length,
        cast(segment_count as double) as segment_count,
        cast(segment_index as double) as segment_index,
        cast(sequence_index as double) as sequence_index,
        cast(aligned_bases as double) as aligned_bases,
        cast(map_qual as double) as map_qual,
        cast(score as double) as score,
        cast(adj_aligned_bases as double) as adj_aligned_bases,
        cast(avg_fragment_length as double) as avg_fragment_length,
        cast(incomplete_fragments as double) as incomplete_fragments,
        cast(breakend_qual as double) as breakend_qual,
        cast(facing_breakend_ids as varchar) as facing_breakend_ids,
        cast(alt_alignments as varchar) as alt_alignments,
        cast(insertion_type as varchar) as insertion_type,
        cast(unique_frag_pos as double) as unique_frag_pos,
        cast(closest_assembly as varchar) as closest_assembly,
        cast(non_primary_frags as double) as non_primary_frags
    from
        transformed

)

select * from final

