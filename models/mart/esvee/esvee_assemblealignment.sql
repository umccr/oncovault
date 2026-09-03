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
        sat.assembly_ids,
        sat.assembly_info,
        sat.ref_info,
        sat.raw_seq_coords,
        sat.adj_seq_coords,
        sat.map_qual,
        sat.cigar,
        sat.orientation,
        sat.aligned_bases,
        sat.score,
        sat.flags,
        sat.n_matches,
        sat.xa_tag,
        sat.md_tag,
        sat.calc_align_length,
        sat.mod_map_qual,
        sat.dropped_on_requery,
        sat.linked_alt_alignment
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_esvee_assemblealignment') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(assembly_ids as varchar) as assembly_ids,
        cast(assembly_info as varchar) as assembly_info,
        cast(ref_info as varchar) as ref_info,
        cast(raw_seq_coords as varchar) as raw_seq_coords,
        cast(adj_seq_coords as varchar) as adj_seq_coords,
        cast(map_qual as double) as map_qual,
        cast(cigar as varchar) as cigar,
        cast(orientation as double) as orientation,
        cast(aligned_bases as double) as aligned_bases,
        cast(score as double) as score,
        cast(flags as double) as flags,
        cast(n_matches as double) as n_matches,
        cast(xa_tag as varchar) as xa_tag,
        cast(md_tag as varchar) as md_tag,
        cast(calc_align_length as double) as calc_align_length,
        cast(mod_map_qual as double) as mod_map_qual,
        cast(dropped_on_requery as varchar) as dropped_on_requery,
        cast(linked_alt_alignment as varchar) as linked_alt_alignment
    from
        transformed

)

select * from final

