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
        sat.purity,
        sat.norm_factor,
        sat.fit_score,
        sat.diploid_proportion,
        sat.ploidy,
        sat.gender,
        sat.status,
        sat.polyclonal_proportion,
        sat.purity_min,
        sat.purity_max,
        sat.ploidy_min,
        sat.ploidy_max,
        sat.diploid_proportion_min,
        sat.diploid_proportion_max,
        sat.somatic_penalty,
        sat.whole_genome_duplication,
        sat.ms_indels_per_mb,
        sat.ms_status,
        sat.tml,
        sat.tml_status,
        sat.tmb_per_mb,
        sat.tmb_status,
        sat.tmb_sv,
        sat.run_mode,
        sat.targeted
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_purple_puritytsv') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(purity as double) as purity,
        cast(norm_factor as double) as norm_factor,
        cast(fit_score as double) as fit_score,
        cast(diploid_proportion as double) as diploid_proportion,
        cast(ploidy as double) as ploidy,
        cast(gender as varchar) as gender,
        cast(status as varchar) as status,
        cast(polyclonal_proportion as double) as polyclonal_proportion,
        cast(purity_min as double) as purity_min,
        cast(purity_max as double) as purity_max,
        cast(ploidy_min as double) as ploidy_min,
        cast(ploidy_max as double) as ploidy_max,
        cast(diploid_proportion_min as double) as diploid_proportion_min,
        cast(diploid_proportion_max as double) as diploid_proportion_max,
        cast(somatic_penalty as double) as somatic_penalty,
        cast(whole_genome_duplication as varchar) as whole_genome_duplication,
        cast(ms_indels_per_mb as double) as ms_indels_per_mb,
        cast(ms_status as varchar) as ms_status,
        cast(tml as double) as tml,
        cast(tml_status as varchar) as tml_status,
        cast(tmb_per_mb as double) as tmb_per_mb,
        cast(tmb_status as varchar) as tmb_status,
        cast(tmb_sv as double) as tmb_sv,
        cast(run_mode as varchar) as run_mode,
        cast(targeted as varchar) as targeted
    from
        transformed

)

select * from final
