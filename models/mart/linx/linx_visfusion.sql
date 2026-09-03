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
        sat.cluster_id,
        sat.reportable,
        sat.gene_name_up,
        sat.transcript_up,
        sat.chr_up,
        sat.pos_up,
        sat.strand_up,
        sat.region_type_up,
        sat.fused_exon_up,
        sat.gene_name_down,
        sat.transcript_down,
        sat.chr_down,
        sat.pos_down,
        sat.strand_down,
        sat.region_type_down,
        sat.fused_exon_down
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_linx_visfusion') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(cluster_id as varchar) as cluster_id,
        cast(reportable as varchar) as reportable,
        cast(gene_name_up as varchar) as gene_name_up,
        cast(transcript_up as varchar) as transcript_up,
        cast(chr_up as varchar) as chr_up,
        cast(pos_up as varchar) as pos_up,
        cast(strand_up as varchar) as strand_up,
        cast(region_type_up as varchar) as region_type_up,
        cast(fused_exon_up as varchar) as fused_exon_up,
        cast(gene_name_down as varchar) as gene_name_down,
        cast(transcript_down as varchar) as transcript_down,
        cast(chr_down as varchar) as chr_down,
        cast(pos_down as varchar) as pos_down,
        cast(strand_down as varchar) as strand_down,
        cast(region_type_down as varchar) as region_type_down,
        cast(fused_exon_down as varchar) as fused_exon_down
    from
        transformed

)

select * from final
