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

{% set source_table = 'neo_predictions' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        ne_id,
        variant_type,
        variant_info,
        gene_name,
        aa_up,
        aa_novel,
        aa_down,
        peptide_count,
        tpm_source,
        rna_frags,
        rna_depth,
        tpm_up,
        tpm_down,
        tpm_expected,
        tpm_raw_effective,
        tpm_effective,
        tpm_cancer_up,
        tpm_cancer_down,
        tpm_pancancer_up,
        tpm_pancancer_down,
        nmd_min,
        nmd_max,
        coding_bases_length_min,
        coding_bases_length_max,
        fused_intron_length,
        skipped_donors,
        skipped_acceptors,
        transcripts_up,
        transcripts_down,
        variant_cn,
        cn,
        subclonal_likelihood

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
                'ne_id',
                'variant_type',
                'variant_info',
                'gene_name',
                'aa_up',
                'aa_novel',
                'aa_down',
                'peptide_count',
                'tpm_source',
                'rna_frags',
                'rna_depth',
                'tpm_up',
                'tpm_down',
                'tpm_expected',
                'tpm_raw_effective',
                'tpm_effective',
                'tpm_cancer_up',
                'tpm_cancer_down',
                'tpm_pancancer_up',
                'tpm_pancancer_down',
                'nmd_min',
                'nmd_max',
                'coding_bases_length_min',
                'coding_bases_length_max',
                'fused_intron_length',
                'skipped_donors',
                'skipped_acceptors',
                'transcripts_up',
                'transcripts_down',
                'variant_cn',
                'cn',
                'subclonal_likelihood'
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

        cast(ne_id as bigint) as ne_id,
        cast(variant_type as varchar) as variant_type,
        cast(variant_info as varchar) as variant_info,
        cast(gene_name as varchar) as gene_name,
        cast(aa_up as varchar) as aa_up,
        cast(aa_novel as varchar) as aa_novel,
        cast(aa_down as varchar) as aa_down,
        cast(peptide_count as double) as peptide_count,
        cast(tpm_source as varchar) as tpm_source,
        cast(rna_frags as double) as rna_frags,
        cast(rna_depth as double) as rna_depth,
        cast(tpm_up as double) as tpm_up,
        cast(tpm_down as double) as tpm_down,
        cast(tpm_expected as double) as tpm_expected,
        cast(tpm_raw_effective as double) as tpm_raw_effective,
        cast(tpm_effective as double) as tpm_effective,
        cast(tpm_cancer_up as double) as tpm_cancer_up,
        cast(tpm_cancer_down as double) as tpm_cancer_down,
        cast(tpm_pancancer_up as double) as tpm_pancancer_up,
        cast(tpm_pancancer_down as double) as tpm_pancancer_down,
        cast(nmd_min as double) as nmd_min,
        cast(nmd_max as double) as nmd_max,
        cast(coding_bases_length_min as double) as coding_bases_length_min,
        cast(coding_bases_length_max as double) as coding_bases_length_max,
        cast(fused_intron_length as double) as fused_intron_length,
        cast(skipped_donors as double) as skipped_donors,
        cast(skipped_acceptors as double) as skipped_acceptors,
        cast(transcripts_up as varchar) as transcripts_up,
        cast(transcripts_down as varchar) as transcripts_down,
        cast(variant_cn as double) as variant_cn,
        cast(cn as double) as cn,
        cast(subclonal_likelihood as double) as subclonal_likelihood

    from
        transformed

)

select * from final
