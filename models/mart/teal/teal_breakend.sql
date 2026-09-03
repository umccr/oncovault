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
        sat.chrom,
        sat."position",
        sat.orientation,
        sat.cg_rich,
        sat.filter,
        sat.in_tumor,
        sat.in_germline,
        sat.distance_to_telomere,
        sat.max_telomeric_length,
        sat.max_anchor_length,
        sat.tumor_sr_tel_dp_tel,
        sat.tumor_sr_tel_dp_no_tel,
        sat.tumor_sr_tel_no_dp,
        sat.tumor_sr_no_tel_dp_tel,
        sat.tumor_dp_tel_no_sr,
        sat.tumor_total_support,
        sat.tumor_mapq,
        sat.germ_sr_tel_dp_tel,
        sat.germ_sr_tel_dp_no_tel,
        sat.germ_sr_tel_no_dp,
        sat.germ_sr_no_tel_dp_tel,
        sat.germ_dp_tel_no_sr,
        sat.germ_total_support,
        sat.germ_mapq
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_teal_breakend') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(chrom as varchar) as chrom,
        cast("position" as double) as "position",
        cast(orientation as double) as orientation,
        cast(cg_rich as varchar) as cg_rich,
        cast(filter as varchar) as filter,
        cast(in_tumor as varchar) as in_tumor,
        cast(in_germline as varchar) as in_germline,
        cast(distance_to_telomere as double) as distance_to_telomere,
        cast(max_telomeric_length as double) as max_telomeric_length,
        cast(max_anchor_length as double) as max_anchor_length,
        cast(tumor_sr_tel_dp_tel as double) as tumor_sr_tel_dp_tel,
        cast(tumor_sr_tel_dp_no_tel as double) as tumor_sr_tel_dp_no_tel,
        cast(tumor_sr_tel_no_dp as double) as tumor_sr_tel_no_dp,
        cast(tumor_sr_no_tel_dp_tel as double) as tumor_sr_no_tel_dp_tel,
        cast(tumor_dp_tel_no_sr as double) as tumor_dp_tel_no_sr,
        cast(tumor_total_support as double) as tumor_total_support,
        cast(tumor_mapq as double) as tumor_mapq,
        cast(germ_sr_tel_dp_tel as double) as germ_sr_tel_dp_tel,
        cast(germ_sr_tel_dp_no_tel as double) as germ_sr_tel_dp_no_tel,
        cast(germ_sr_tel_no_dp as double) as germ_sr_tel_no_dp,
        cast(germ_sr_no_tel_dp_tel as double) as germ_sr_no_tel_dp_tel,
        cast(germ_dp_tel_no_sr as double) as germ_dp_tel_no_sr,
        cast(germ_total_support as double) as germ_total_support,
        cast(germ_mapq as double) as germ_mapq
    from
        transformed

)

select * from final

