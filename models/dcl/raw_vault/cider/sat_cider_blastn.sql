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

{% set source_table = 'cider_blastn' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        cdr3_seq,
        cdr3_aa,
        match_type,
        gene,
        functionality,
        p_ident,
        seq_length,
        align_start,
        align_end,
        align_gaps,
        align_evalue,
        align_bitscore,
        ref_strand,
        ref_start,
        ref_end,
        ref_contig,
        ref_seq

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
                'cdr3_seq',
                'cdr3_aa',
                'match_type',
                'gene',
                'functionality',
                'p_ident',
                'seq_length',
                'align_start',
                'align_end',
                'align_gaps',
                'align_evalue',
                'align_bitscore',
                'ref_strand',
                'ref_start',
                'ref_end',
                'ref_contig',
                'ref_seq'
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

        cast(cdr3_seq as varchar) as cdr3_seq,
        cast(cdr3_aa as varchar) as cdr3_aa,
        cast(match_type as varchar) as match_type,
        cast(gene as varchar) as gene,
        cast(functionality as varchar) as functionality,
        cast(p_ident as double) as p_ident,
        cast(seq_length as double) as seq_length,
        cast(align_start as double) as align_start,
        cast(align_end as double) as align_end,
        cast(align_gaps as double) as align_gaps,
        cast(align_evalue as double) as align_evalue,
        cast(align_bitscore as double) as align_bitscore,
        cast(ref_strand as varchar) as ref_strand,
        cast(ref_start as double) as ref_start,
        cast(ref_end as double) as ref_end,
        cast(ref_contig as varchar) as ref_contig,
        cast(ref_seq as varchar) as ref_seq

    from
        transformed

)

select * from final
