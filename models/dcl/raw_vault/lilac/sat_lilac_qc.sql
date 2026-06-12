{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['library_workflow_run_hk', 'load_date', 'hash_diff'],
        merge_update_columns=['last_seen_datetime'],
        on_schema_change='fail',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
        partitioned_by=['load_date']
    )
}}

{% set source_table = 'lilac_qc' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        status,
        score_margin,
        next_solution_alleles,
        median_base_quality,
        hla_y_allele,
        discarded_indels,
        discarded_indel_max_frags,
        discarded_alignment_fragments,
        a_low_coverage_bases,
        b_low_coverage_bases,
        c_low_coverage_bases,
        a_types,
        b_types,
        c_types,
        total_fragments,
        fitted_fragments,
        unmatched_fragments,
        uninformative_fragments,
        hla_y_fragments,
        percent_unique,
        percent_shared,
        percent_wildcard,
        unused_amino_acids,
        unused_amino_acid_max_frags,
        unused_haplotypes,
        unused_haplotype_max_frags,
        somatic_variants_matched,
        somatic_variants_unmatched

    from
        {{ source('tidywigits', source_table) }}

    {% if is_incremental() %}

    where
        regexp_like("$path", '{{ source_table }}\.parquet$')
        and batch_date >= date_format(( select max(load_date) from {{ this }} ), '%Y-%m-%d')

    {% else %}

    where
        regexp_like("$path", '{{ source_table }}\.parquet$')
        and batch_date >= '{{ var("initial_batch_date") }}'

    {% endif %}

),

encoded as (

    select
        lower(to_hex(sha256(cast(portal_run_id as varbinary)))) as workflow_run_hk,
        lower(to_hex(sha256(cast(library_id as varbinary)))) as library_hk,
        {{
            generate_hash_diff([
                'batch_id',
                'batch_date',
                'status',
                'score_margin',
                'next_solution_alleles',
                'median_base_quality',
                'hla_y_allele',
                'discarded_indels',
                'discarded_indel_max_frags',
                'discarded_alignment_fragments',
                'a_low_coverage_bases',
                'b_low_coverage_bases',
                'c_low_coverage_bases',
                'a_types',
                'b_types',
                'c_types',
                'total_fragments',
                'fitted_fragments',
                'unmatched_fragments',
                'uninformative_fragments',
                'hla_y_fragments',
                'percent_unique',
                'percent_shared',
                'percent_wildcard',
                'unused_amino_acids',
                'unused_amino_acid_max_frags',
                'unused_haplotypes',
                'unused_haplotype_max_frags',
                'somatic_variants_matched',
                'somatic_variants_unmatched'
            ])
        }} as hash_diff,
        *
    from
        source

),

transformed as (

    select
        lower(to_hex(sha256(cast(concat(workflow_run_hk, library_hk) as varbinary)))) as library_workflow_run_hk,
        cast('{{ run_started_at.strftime("%Y-%m-%d") }}' as date) as load_date,
        'tidywigits_{{ source_table }}' as record_source,
        cast('{{ run_started_at }}' as timestamp) as last_seen_datetime,
        *
    from
        encoded

),

final as (

    select
        cast(library_workflow_run_hk as varchar(64)) as library_workflow_run_hk,
        cast(load_date as date) as load_date,
        cast(hash_diff as varchar(64)) as hash_diff,
        cast(record_source as varchar(255)) as record_source,
        cast(last_seen_datetime as timestamp) as last_seen_datetime,
        cast(batch_id as varchar(26)) as batch_id,
        cast(batch_date as date) as batch_date,

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
