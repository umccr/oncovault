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

{% set source_table = 'linx_breakends' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        bnd_id,
        sv_id,
        is_start,
        gene,
        transcript_id,
        canonical,
        gene_orientation,
        disruptive,
        reported_disruption,
        undisrupted_cn,
        region_type,
        coding_type,
        biotype,
        exonic_basephase,
        next_splice_exon_rank,
        next_splice_exon_phase,
        next_splice_distance,
        total_exon_count,
        exon_up,
        exon_down

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
                'bnd_id',
                'sv_id',
                'is_start',
                'gene',
                'transcript_id',
                'canonical',
                'gene_orientation',
                'disruptive',
                'reported_disruption',
                'undisrupted_cn',
                'region_type',
                'coding_type',
                'biotype',
                'exonic_basephase',
                'next_splice_exon_rank',
                'next_splice_exon_phase',
                'next_splice_distance',
                'total_exon_count',
                'exon_up',
                'exon_down'
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

        cast(bnd_id as varchar) as bnd_id,
        cast(sv_id as varchar) as sv_id,
        cast(is_start as varchar) as is_start,
        cast(gene as varchar) as gene,
        cast(transcript_id as varchar) as transcript_id,
        cast(canonical as varchar) as canonical,
        cast(gene_orientation as varchar) as gene_orientation,
        cast(disruptive as varchar) as disruptive,
        cast(reported_disruption as varchar) as reported_disruption,
        cast(undisrupted_cn as double) as undisrupted_cn,
        cast(region_type as varchar) as region_type,
        cast(coding_type as varchar) as coding_type,
        cast(biotype as varchar) as biotype,
        cast(exonic_basephase as double) as exonic_basephase,
        cast(next_splice_exon_rank as double) as next_splice_exon_rank,
        cast(next_splice_exon_phase as double) as next_splice_exon_phase,
        cast(next_splice_distance as double) as next_splice_distance,
        cast(total_exon_count as double) as total_exon_count,
        cast(exon_up as double) as exon_up,
        cast(exon_down as double) as exon_down

    from
        transformed

)

select * from final
