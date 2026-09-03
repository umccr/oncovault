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
        sat.taxid,
        sat."name",
        sat.qc_status,
        sat.integrations,
        sat.interpretation,
        sat.percentage_covered,
        sat.mean_coverage,
        sat.expected_clonal_coverage,
        sat.reported,
        sat.blacklisted,
        sat.driver_likelihood
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_virusinterpreter_annotated') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(taxid as varchar) as taxid,
        cast("name" as varchar) as "name",
        cast(qc_status as varchar) as qc_status,
        cast(integrations as double) as integrations,
        cast(interpretation as varchar) as interpretation,
        cast(percentage_covered as double) as percentage_covered,
        cast(mean_coverage as double) as mean_coverage,
        cast(expected_clonal_coverage as varchar) as expected_clonal_coverage,
        cast(reported as varchar) as reported,
        cast(blacklisted as varchar) as blacklisted,
        cast(driver_likelihood as varchar) as driver_likelihood
    from
        transformed

)

select * from final

