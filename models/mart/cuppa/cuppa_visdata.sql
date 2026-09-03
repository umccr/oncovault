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
        sat.sample_id,
        sat.data_type,
        sat.clf_group,
        sat.clf_name,
        sat.feat_name,
        sat.feat_value,
        sat.cancer_type,
        sat.data_value,
        sat.rank,
        sat.rank_group
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_cuppa_visdata') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(sample_id as varchar) as sample_id,
        cast(data_type as varchar) as data_type,
        cast(clf_group as varchar) as clf_group,
        cast(clf_name as varchar) as clf_name,
        cast(feat_name as varchar) as feat_name,
        cast(feat_value as double) as feat_value,
        cast(cancer_type as varchar) as cancer_type,
        cast(data_value as double) as data_value,
        cast(rank as double) as rank,
        cast(rank_group as double) as rank_group
    from
        transformed

)

select * from final

