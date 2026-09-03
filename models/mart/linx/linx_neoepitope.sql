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
        sat.gene_id_up,
        sat.gene_name_up,
        sat.chrom_up,
        sat.pos_up,
        sat.orientation_up,
        sat.sv_id_up,
        sat.gene_id_down,
        sat.gene_name_down,
        sat.chrom_down,
        sat.pos_down,
        sat.orientation_down,
        sat.sv_id_down,
        sat.junc_cn,
        sat.cn,
        sat.insert_seq,
        sat.chain_length,
        sat.transcripts_up,
        sat.transcripts_down
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_linx_neoepitope') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(gene_id_up as varchar) as gene_id_up,
        cast(gene_name_up as varchar) as gene_name_up,
        cast(chrom_up as varchar) as chrom_up,
        cast(pos_up as double) as pos_up,
        cast(orientation_up as double) as orientation_up,
        cast(sv_id_up as varchar) as sv_id_up,
        cast(gene_id_down as varchar) as gene_id_down,
        cast(gene_name_down as varchar) as gene_name_down,
        cast(chrom_down as varchar) as chrom_down,
        cast(pos_down as double) as pos_down,
        cast(orientation_down as double) as orientation_down,
        cast(sv_id_down as varchar) as sv_id_down,
        cast(junc_cn as double) as junc_cn,
        cast(cn as double) as cn,
        cast(insert_seq as varchar) as insert_seq,
        cast(chain_length as double) as chain_length,
        cast(transcripts_up as varchar) as transcripts_up,
        cast(transcripts_down as varchar) as transcripts_down
    from
        transformed

)

select * from final
