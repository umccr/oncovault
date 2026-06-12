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

{% set source_table = 'esvee_assemblebreakend' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        id,
        phase_group_id,
        phase_set_id,
        assembly_id,
        mate_id,
        assembly_info,
        "type",
        chrom,
        "position",
        orientation,
        mate_chr,
        mate_pos,
        mate_orient,
        "length",
        inserted_bases,
        homology,
        confidence_interval,
        inexact_offset,
        qual,
        split_fragments,
        ref_split_fragments,
        disc_fragments,
        ref_disc_fragments,
        forward_reads,
        reverse_reads,
        sequence_length,
        segment_count,
        segment_index,
        sequence_index,
        aligned_bases,
        map_qual,
        score,
        adj_aligned_bases,
        avg_fragment_length,
        incomplete_fragments,
        breakend_qual,
        facing_breakend_ids,
        alt_alignments,
        insertion_type,
        unique_frag_pos,
        closest_assembly,
        non_primary_frags

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
                'phase_group_id',
                'phase_set_id',
                'assembly_id',
                'mate_id',
                'assembly_info',
                '"type"',
                'chrom',
                '"position"',
                'orientation',
                'mate_chr',
                'mate_pos',
                'mate_orient',
                '"length"',
                'inserted_bases',
                'homology',
                'confidence_interval',
                'inexact_offset',
                'qual',
                'split_fragments',
                'ref_split_fragments',
                'disc_fragments',
                'ref_disc_fragments',
                'forward_reads',
                'reverse_reads',
                'sequence_length',
                'segment_count',
                'segment_index',
                'sequence_index',
                'aligned_bases',
                'map_qual',
                'score',
                'adj_aligned_bases',
                'avg_fragment_length',
                'incomplete_fragments',
                'breakend_qual',
                'facing_breakend_ids',
                'alt_alignments',
                'insertion_type',
                'unique_frag_pos',
                'closest_assembly',
                'non_primary_frags'
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
        cast(phase_group_id as varchar) as phase_group_id,
        cast(phase_set_id as varchar) as phase_set_id,
        cast(assembly_id as varchar) as assembly_id,
        cast(mate_id as varchar) as mate_id,
        cast(assembly_info as varchar) as assembly_info,
        cast("type" as varchar) as "type",
        cast(chrom as varchar) as chrom,
        cast("position" as double) as "position",
        cast(orientation as double) as orientation,
        cast(mate_chr as varchar) as mate_chr,
        cast(mate_pos as double) as mate_pos,
        cast(mate_orient as double) as mate_orient,
        cast("length" as double) as "length",
        cast(inserted_bases as varchar) as inserted_bases,
        cast(homology as varchar) as homology,
        cast(confidence_interval as varchar) as confidence_interval,
        cast(inexact_offset as varchar) as inexact_offset,
        cast(qual as double) as qual,
        cast(split_fragments as double) as split_fragments,
        cast(ref_split_fragments as double) as ref_split_fragments,
        cast(disc_fragments as double) as disc_fragments,
        cast(ref_disc_fragments as double) as ref_disc_fragments,
        cast(forward_reads as double) as forward_reads,
        cast(reverse_reads as double) as reverse_reads,
        cast(sequence_length as double) as sequence_length,
        cast(segment_count as double) as segment_count,
        cast(segment_index as double) as segment_index,
        cast(sequence_index as double) as sequence_index,
        cast(aligned_bases as double) as aligned_bases,
        cast(map_qual as double) as map_qual,
        cast(score as double) as score,
        cast(adj_aligned_bases as double) as adj_aligned_bases,
        cast(avg_fragment_length as double) as avg_fragment_length,
        cast(incomplete_fragments as double) as incomplete_fragments,
        cast(breakend_qual as double) as breakend_qual,
        cast(facing_breakend_ids as varchar) as facing_breakend_ids,
        cast(alt_alignments as varchar) as alt_alignments,
        cast(insertion_type as varchar) as insertion_type,
        cast(unique_frag_pos as double) as unique_frag_pos,
        cast(closest_assembly as varchar) as closest_assembly,
        cast(non_primary_frags as double) as non_primary_frags

    from
        transformed

)

select * from final
