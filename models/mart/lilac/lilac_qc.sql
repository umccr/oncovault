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
        sat.status,
        sat.score_margin,
        sat.next_solution_alleles,
        sat.median_base_quality,
        sat.hla_y_allele,
        sat.discarded_indels,
        sat.discarded_indel_max_frags,
        sat.discarded_alignment_fragments,
        sat.a_low_coverage_bases,
        sat.b_low_coverage_bases,
        sat.c_low_coverage_bases,
        sat.a_types,
        sat.b_types,
        sat.c_types,
        sat.total_fragments,
        sat.fitted_fragments,
        sat.unmatched_fragments,
        sat.uninformative_fragments,
        sat.hla_y_fragments,
        sat.percent_unique,
        sat.percent_shared,
        sat.percent_wildcard,
        sat.unused_amino_acids,
        sat.unused_amino_acid_max_frags,
        sat.unused_haplotypes,
        sat.unused_haplotype_max_frags,
        sat.somatic_variants_matched,
        sat.somatic_variants_unmatched
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_lilac_qc') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

        cast(status as varchar) as status,
        cast(score_margin as double) as score_margin,
        cast(next_solution_alleles as varchar) as next_solution_alleles,
        cast(median_base_quality as double) as median_base_quality,
        cast(hla_y_allele as varchar) as hla_y_allele,
        cast(discarded_indels as double) as discarded_indels,
        cast(discarded_indel_max_frags as double) as discarded_indel_max_frags,
        cast(discarded_alignment_fragments as double) as discarded_alignment_fragments,
        cast(a_low_coverage_bases as double) as a_low_coverage_bases,
        cast(b_low_coverage_bases as double) as b_low_coverage_bases,
        cast(c_low_coverage_bases as double) as c_low_coverage_bases,
        cast(a_types as double) as a_types,
        cast(b_types as double) as b_types,
        cast(c_types as double) as c_types,
        cast(total_fragments as double) as total_fragments,
        cast(fitted_fragments as double) as fitted_fragments,
        cast(unmatched_fragments as double) as unmatched_fragments,
        cast(uninformative_fragments as double) as uninformative_fragments,
        cast(hla_y_fragments as double) as hla_y_fragments,
        cast(percent_unique as double) as percent_unique,
        cast(percent_shared as double) as percent_shared,
        cast(percent_wildcard as double) as percent_wildcard,
        cast(unused_amino_acids as double) as unused_amino_acids,
        cast(unused_amino_acid_max_frags as double) as unused_amino_acid_max_frags,
        cast(unused_haplotypes as double) as unused_haplotypes,
        cast(unused_haplotype_max_frags as double) as unused_haplotype_max_frags,
        cast(somatic_variants_matched as double) as somatic_variants_matched,
        cast(somatic_variants_unmatched as double) as somatic_variants_unmatched
    from
        transformed

)

select * from final

