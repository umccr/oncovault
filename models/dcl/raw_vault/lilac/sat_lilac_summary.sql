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

{% set source_table = 'lilac_summary' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        allele,
        ref_total,
        ref_unique,
        ref_shared,
        ref_wild,
        tumor_total,
        tumor_unique,
        tumor_shared,
        tumor_wild,
        rna_total,
        rna_unique,
        rna_shared,
        rna_wild,
        tumor_cn,
        somatic_missense,
        somatic_nonsense_or_frameshift,
        somatic_splice,
        somatic_synonymous,
        somatic_inframe_indel

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
                'allele',
                'ref_total',
                'ref_unique',
                'ref_shared',
                'ref_wild',
                'tumor_total',
                'tumor_unique',
                'tumor_shared',
                'tumor_wild',
                'rna_total',
                'rna_unique',
                'rna_shared',
                'rna_wild',
                'tumor_cn',
                'somatic_missense',
                'somatic_nonsense_or_frameshift',
                'somatic_splice',
                'somatic_synonymous',
                'somatic_inframe_indel'
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

        cast(allele as varchar) as allele,
        cast(ref_total as double) as ref_total,
        cast(ref_unique as double) as ref_unique,
        cast(ref_shared as double) as ref_shared,
        cast(ref_wild as double) as ref_wild,
        cast(tumor_total as double) as tumor_total,
        cast(tumor_unique as double) as tumor_unique,
        cast(tumor_shared as double) as tumor_shared,
        cast(tumor_wild as double) as tumor_wild,
        cast(rna_total as double) as rna_total,
        cast(rna_unique as double) as rna_unique,
        cast(rna_shared as double) as rna_shared,
        cast(rna_wild as double) as rna_wild,
        cast(tumor_cn as double) as tumor_cn,
        cast(somatic_missense as double) as somatic_missense,
        cast(somatic_nonsense_or_frameshift as double) as somatic_nonsense_or_frameshift,
        cast(somatic_splice as double) as somatic_splice,
        cast(somatic_synonymous as double) as somatic_synonymous,
        cast(somatic_inframe_indel as double) as somatic_inframe_indel

    from
        transformed

)

select * from final
