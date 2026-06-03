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

{% set source_table = 'cider_vdj' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_pfix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        cdr3_seq,
        cdr3_aa,
        locus,
        filter,
        blastn_status,
        min_high_qual_base_reads,
        assigned_reads,
        v_aligned_reads,
        j_aligned_reads,
        in_frame,
        contains_stop,
        v_type,
        v_anchor_start,
        v_anchor_end,
        v_anchor_seq,
        v_anchor_template_seq,
        v_anchor_aa,
        v_anchor_template_aa,
        v_match_method,
        v_similarity_score,
        v_non_split_reads,
        j_type,
        j_anchor_start,
        j_anchor_end,
        j_anchor_seq,
        j_anchor_template_seq,
        j_anchor_aa,
        j_anchor_template_aa,
        j_match_method,
        j_similarity_score,
        j_non_split_reads,
        v_gene,
        v_pident,
        v_align_start,
        v_align_end,
        d_gene,
        d_pident,
        d_align_start,
        d_align_end,
        j_gene,
        j_pident,
        j_align_start,
        j_align_end,
        v_primer_matches,
        j_primer_matches,
        layout_id,
        full_seq,
        support

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
                'locus',
                'filter',
                'blastn_status',
                'min_high_qual_base_reads',
                'assigned_reads',
                'v_aligned_reads',
                'j_aligned_reads',
                'in_frame',
                'contains_stop',
                'v_type',
                'v_anchor_start',
                'v_anchor_end',
                'v_anchor_seq',
                'v_anchor_template_seq',
                'v_anchor_aa',
                'v_anchor_template_aa',
                'v_match_method',
                'v_similarity_score',
                'v_non_split_reads',
                'j_type',
                'j_anchor_start',
                'j_anchor_end',
                'j_anchor_seq',
                'j_anchor_template_seq',
                'j_anchor_aa',
                'j_anchor_template_aa',
                'j_match_method',
                'j_similarity_score',
                'j_non_split_reads',
                'v_gene',
                'v_pident',
                'v_align_start',
                'v_align_end',
                'd_gene',
                'd_pident',
                'd_align_start',
                'd_align_end',
                'j_gene',
                'j_pident',
                'j_align_start',
                'j_align_end',
                'v_primer_matches',
                'j_primer_matches',
                'layout_id',
                'full_seq',
                'support'
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
        cast(locus as varchar) as locus,
        cast(filter as varchar) as filter,
        cast(blastn_status as varchar) as blastn_status,
        cast(min_high_qual_base_reads as double) as min_high_qual_base_reads,
        cast(assigned_reads as double) as assigned_reads,
        cast(v_aligned_reads as double) as v_aligned_reads,
        cast(j_aligned_reads as double) as j_aligned_reads,
        cast(in_frame as varchar) as in_frame,
        cast(contains_stop as varchar) as contains_stop,
        cast(v_type as varchar) as v_type,
        cast(v_anchor_start as double) as v_anchor_start,
        cast(v_anchor_end as double) as v_anchor_end,
        cast(v_anchor_seq as varchar) as v_anchor_seq,
        cast(v_anchor_template_seq as varchar) as v_anchor_template_seq,
        cast(v_anchor_aa as varchar) as v_anchor_aa,
        cast(v_anchor_template_aa as varchar) as v_anchor_template_aa,
        cast(v_match_method as varchar) as v_match_method,
        cast(v_similarity_score as double) as v_similarity_score,
        cast(v_non_split_reads as double) as v_non_split_reads,
        cast(j_type as varchar) as j_type,
        cast(j_anchor_start as double) as j_anchor_start,
        cast(j_anchor_end as double) as j_anchor_end,
        cast(j_anchor_seq as varchar) as j_anchor_seq,
        cast(j_anchor_template_seq as varchar) as j_anchor_template_seq,
        cast(j_anchor_aa as varchar) as j_anchor_aa,
        cast(j_anchor_template_aa as varchar) as j_anchor_template_aa,
        cast(j_match_method as varchar) as j_match_method,
        cast(j_similarity_score as double) as j_similarity_score,
        cast(j_non_split_reads as double) as j_non_split_reads,
        cast(v_gene as varchar) as v_gene,
        cast(v_pident as double) as v_pident,
        cast(v_align_start as double) as v_align_start,
        cast(v_align_end as double) as v_align_end,
        cast(d_gene as varchar) as d_gene,
        cast(d_pident as double) as d_pident,
        cast(d_align_start as double) as d_align_start,
        cast(d_align_end as double) as d_align_end,
        cast(j_gene as varchar) as j_gene,
        cast(j_pident as double) as j_pident,
        cast(j_align_start as double) as j_align_start,
        cast(j_align_end as double) as j_align_end,
        cast(v_primer_matches as double) as v_primer_matches,
        cast(j_primer_matches as double) as j_primer_matches,
        cast(layout_id as varchar) as layout_id,
        cast(full_seq as varchar) as full_seq,
        cast(support as varchar) as support

    from
        transformed

)

select * from final
