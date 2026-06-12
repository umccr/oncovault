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

{% set source_table = 'neo_candidates' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        ne_id,
        variant_type,
        variant_info,
        variant_cn,
        cn,
        subclonal_likelihood,
        gene_id_up,
        gene_id_down,
        gene_name_up,
        gene_name_down,
        chr_up,
        chr_down,
        orient_up,
        orient_down,
        aa_upstream,
        aa_downstream,
        aa_novel,
        nmd_min,
        nmd_max,
        coding_bases_length_min,
        coding_bases_length_max,
        fused_intron_length,
        skipped_donors,
        skipped_acceptors,
        transcripts_up,
        transcripts_down,
        aa_wildtype,
        coding_base_up_pos_start,
        coding_base_up_pos_end,
        coding_bases_up,
        coding_base_cigar_up,
        coding_base_down_pos_start,
        coding_base_down_pos_end,
        coding_bases_down,
        coding_base_cigar_down

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
                'variant_cn',
                'cn',
                'subclonal_likelihood',
                'gene_id_up',
                'gene_id_down',
                'gene_name_up',
                'gene_name_down',
                'chr_up',
                'chr_down',
                'orient_up',
                'orient_down',
                'aa_upstream',
                'aa_downstream',
                'aa_novel',
                'nmd_min',
                'nmd_max',
                'coding_bases_length_min',
                'coding_bases_length_max',
                'fused_intron_length',
                'skipped_donors',
                'skipped_acceptors',
                'transcripts_up',
                'transcripts_down',
                'aa_wildtype',
                'coding_base_up_pos_start',
                'coding_base_up_pos_end',
                'coding_bases_up',
                'coding_base_cigar_up',
                'coding_base_down_pos_start',
                'coding_base_down_pos_end',
                'coding_bases_down',
                'coding_base_cigar_down'
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
        cast(variant_cn as double) as variant_cn,
        cast(cn as double) as cn,
        cast(subclonal_likelihood as double) as subclonal_likelihood,
        cast(gene_id_up as varchar) as gene_id_up,
        cast(gene_id_down as varchar) as gene_id_down,
        cast(gene_name_up as varchar) as gene_name_up,
        cast(gene_name_down as varchar) as gene_name_down,
        cast(chr_up as varchar) as chr_up,
        cast(chr_down as varchar) as chr_down,
        cast(orient_up as double) as orient_up,
        cast(orient_down as double) as orient_down,
        cast(aa_upstream as varchar) as aa_upstream,
        cast(aa_downstream as varchar) as aa_downstream,
        cast(aa_novel as varchar) as aa_novel,
        cast(nmd_min as double) as nmd_min,
        cast(nmd_max as double) as nmd_max,
        cast(coding_bases_length_min as double) as coding_bases_length_min,
        cast(coding_bases_length_max as double) as coding_bases_length_max,
        cast(fused_intron_length as double) as fused_intron_length,
        cast(skipped_donors as double) as skipped_donors,
        cast(skipped_acceptors as double) as skipped_acceptors,
        cast(transcripts_up as varchar) as transcripts_up,
        cast(transcripts_down as varchar) as transcripts_down,
        cast(aa_wildtype as varchar) as aa_wildtype,
        cast(coding_base_up_pos_start as double) as coding_base_up_pos_start,
        cast(coding_base_up_pos_end as double) as coding_base_up_pos_end,
        cast(coding_bases_up as varchar) as coding_bases_up,
        cast(coding_base_cigar_up as varchar) as coding_base_cigar_up,
        cast(coding_base_down_pos_start as double) as coding_base_down_pos_start,
        cast(coding_base_down_pos_end as double) as coding_base_down_pos_end,
        cast(coding_bases_down as varchar) as coding_bases_down,
        cast(coding_base_cigar_down as varchar) as coding_base_cigar_down

    from
        transformed

)

select * from final
