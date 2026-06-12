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

{% set source_table = 'esvee_assembleassembly' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        id,
        chrom,
        junc_position,
        junc_orientation,
        junc_type,
        ext_base_length,
        ref_base_position,
        ref_base_length,
        ref_base_cigar,
        split_frags,
        ref_split_frags,
        disc_frags,
        ref_disc_frags,
        outcome,
        phase_group_id,
        phase_group_count,
        phase_set_id,
        phase_set_count,
        split_links,
        facing_links,
        sv_type,
        sv_length,
        inserted_bases,
        overlap_bases,
        secondary_links,
        junc_sequence,
        ref_base_sequence,
        insert_type,
        ref_base_candidates,
        unmapped_candidates,
        assembly_info

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
                'id',
                'chrom',
                'junc_position',
                'junc_orientation',
                'junc_type',
                'ext_base_length',
                'ref_base_position',
                'ref_base_length',
                'ref_base_cigar',
                'split_frags',
                'ref_split_frags',
                'disc_frags',
                'ref_disc_frags',
                'outcome',
                'phase_group_id',
                'phase_group_count',
                'phase_set_id',
                'phase_set_count',
                'split_links',
                'facing_links',
                'sv_type',
                'sv_length',
                'inserted_bases',
                'overlap_bases',
                'secondary_links',
                'junc_sequence',
                'ref_base_sequence',
                'insert_type',
                'ref_base_candidates',
                'unmapped_candidates',
                'assembly_info'
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

        cast(id as varchar) as id,
        cast(chrom as varchar) as chrom,
        cast(junc_position as double) as junc_position,
        cast(junc_orientation as double) as junc_orientation,
        cast(junc_type as varchar) as junc_type,
        cast(ext_base_length as double) as ext_base_length,
        cast(ref_base_position as double) as ref_base_position,
        cast(ref_base_length as double) as ref_base_length,
        cast(ref_base_cigar as varchar) as ref_base_cigar,
        cast(split_frags as double) as split_frags,
        cast(ref_split_frags as double) as ref_split_frags,
        cast(disc_frags as double) as disc_frags,
        cast(ref_disc_frags as double) as ref_disc_frags,
        cast(outcome as varchar) as outcome,
        cast(phase_group_id as double) as phase_group_id,
        cast(phase_group_count as double) as phase_group_count,
        cast(phase_set_id as varchar) as phase_set_id,
        cast(phase_set_count as double) as phase_set_count,
        cast(split_links as varchar) as split_links,
        cast(facing_links as varchar) as facing_links,
        cast(sv_type as varchar) as sv_type,
        cast(sv_length as double) as sv_length,
        cast(inserted_bases as varchar) as inserted_bases,
        cast(overlap_bases as varchar) as overlap_bases,
        cast(secondary_links as varchar) as secondary_links,
        cast(junc_sequence as varchar) as junc_sequence,
        cast(ref_base_sequence as varchar) as ref_base_sequence,
        cast(insert_type as varchar) as insert_type,
        cast(ref_base_candidates as double) as ref_base_candidates,
        cast(unmapped_candidates as double) as unmapped_candidates,
        cast(assembly_info as varchar) as assembly_info

    from
        transformed

)

select * from final
