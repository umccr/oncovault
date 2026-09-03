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
        sat.chrom,
        sat.junc_position,
        sat.junc_orientation,
        sat.junc_type,
        sat.ext_base_length,
        sat.ref_base_position,
        sat.ref_base_length,
        sat.ref_base_cigar,
        sat.split_frags,
        sat.ref_split_frags,
        sat.disc_frags,
        sat.ref_disc_frags,
        sat.outcome,
        sat.phase_group_id,
        sat.phase_group_count,
        sat.phase_set_id,
        sat.phase_set_count,
        sat.split_links,
        sat.facing_links,
        sat.sv_type,
        sat.sv_length,
        sat.inserted_bases,
        sat.overlap_bases,
        sat.secondary_links,
        sat.junc_sequence,
        sat.ref_base_sequence,
        sat.insert_type,
        sat.ref_base_candidates,
        sat.unmapped_candidates,
        sat.assembly_info
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_esvee_assembleassembly') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(id as varchar) as id,
        cast(chrom as varchar) as chrom,
        cast(junc_position as double) as junc_position,
        cast(junc_orientation as double) as junc_orientation,
        cast(junc_type as varchar) as junc_type,
        cast(ext_base_length as double) as ext_base_length,
        cast(ref_base_position as double) as ref_base_position,
        cast(ref_base_length as double) as ref_base_length,
        cast(ref_base_cigar as varchar) as ref_base_cigar,
        cast(split_frags as double) as split_frags,
        cast(ref_split_frags as double) as ref_split_frags,
        cast(disc_frags as double) as disc_frags,
        cast(ref_disc_frags as double) as ref_disc_frags,
        cast(outcome as varchar) as outcome,
        cast(phase_group_id as double) as phase_group_id,
        cast(phase_group_count as double) as phase_group_count,
        cast(phase_set_id as varchar) as phase_set_id,
        cast(phase_set_count as double) as phase_set_count,
        cast(split_links as varchar) as split_links,
        cast(facing_links as varchar) as facing_links,
        cast(sv_type as varchar) as sv_type,
        cast(sv_length as double) as sv_length,
        cast(inserted_bases as varchar) as inserted_bases,
        cast(overlap_bases as varchar) as overlap_bases,
        cast(secondary_links as varchar) as secondary_links,
        cast(junc_sequence as varchar) as junc_sequence,
        cast(ref_base_sequence as varchar) as ref_base_sequence,
        cast(insert_type as varchar) as insert_type,
        cast(ref_base_candidates as double) as ref_base_candidates,
        cast(unmapped_candidates as double) as unmapped_candidates,
        cast(assembly_info as varchar) as assembly_info
    from
        transformed

)

select * from final

