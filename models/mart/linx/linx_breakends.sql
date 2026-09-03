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
        sat.bnd_id,
        sat.sv_id,
        sat.is_start,
        sat.gene,
        sat.transcript_id,
        sat.canonical,
        sat.gene_orientation,
        sat.disruptive,
        sat.reported_disruption,
        sat.undisrupted_cn,
        sat.region_type,
        sat.coding_type,
        sat.biotype,
        sat.exonic_basephase,
        sat.next_splice_exon_rank,
        sat.next_splice_exon_phase,
        sat.next_splice_distance,
        sat.total_exon_count,
        sat.exon_up,
        sat.exon_down
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_linx_breakends') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(bnd_id as varchar) as bnd_id,
        cast(sv_id as varchar) as sv_id,
        cast(is_start as varchar) as is_start,
        cast(gene as varchar) as gene,
        cast(transcript_id as varchar) as transcript_id,
        cast(canonical as varchar) as canonical,
        cast(gene_orientation as varchar) as gene_orientation,
        cast(disruptive as varchar) as disruptive,
        cast(reported_disruption as varchar) as reported_disruption,
        cast(undisrupted_cn as double) as undisrupted_cn,
        cast(region_type as varchar) as region_type,
        cast(coding_type as varchar) as coding_type,
        cast(biotype as varchar) as biotype,
        cast(exonic_basephase as double) as exonic_basephase,
        cast(next_splice_exon_rank as double) as next_splice_exon_rank,
        cast(next_splice_exon_phase as double) as next_splice_exon_phase,
        cast(next_splice_distance as double) as next_splice_distance,
        cast(total_exon_count as double) as total_exon_count,
        cast(exon_up as double) as exon_up,
        cast(exon_down as double) as exon_down
    from
        transformed

)

select * from final
