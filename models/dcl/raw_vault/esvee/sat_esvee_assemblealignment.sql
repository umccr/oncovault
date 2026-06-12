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

{% set source_table = 'esvee_assemblealignment' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        assembly_ids,
        assembly_info,
        ref_info,
        raw_seq_coords,
        adj_seq_coords,
        map_qual,
        cigar,
        orientation,
        aligned_bases,
        score,
        flags,
        n_matches,
        xa_tag,
        md_tag,
        calc_align_length,
        mod_map_qual,
        dropped_on_requery,
        linked_alt_alignment

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
                'assembly_ids',
                'assembly_info',
                'ref_info',
                'raw_seq_coords',
                'adj_seq_coords',
                'map_qual',
                'cigar',
                'orientation',
                'aligned_bases',
                'score',
                'flags',
                'n_matches',
                'xa_tag',
                'md_tag',
                'calc_align_length',
                'mod_map_qual',
                'dropped_on_requery',
                'linked_alt_alignment'
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

        cast(assembly_ids as varchar) as assembly_ids,
        cast(assembly_info as varchar) as assembly_info,
        cast(ref_info as varchar) as ref_info,
        cast(raw_seq_coords as varchar) as raw_seq_coords,
        cast(adj_seq_coords as varchar) as adj_seq_coords,
        cast(map_qual as double) as map_qual,
        cast(cigar as varchar) as cigar,
        cast(orientation as double) as orientation,
        cast(aligned_bases as double) as aligned_bases,
        cast(score as double) as score,
        cast(flags as double) as flags,
        cast(n_matches as double) as n_matches,
        cast(xa_tag as varchar) as xa_tag,
        cast(md_tag as varchar) as md_tag,
        cast(calc_align_length as double) as calc_align_length,
        cast(mod_map_qual as double) as mod_map_qual,
        cast(dropped_on_requery as varchar) as dropped_on_requery,
        cast(linked_alt_alignment as varchar) as linked_alt_alignment

    from
        transformed

)

select * from final
