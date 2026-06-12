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

{% set source_table = 'linx_neoepitope' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        gene_id_up,
        gene_name_up,
        chrom_up,
        pos_up,
        orientation_up,
        sv_id_up,

        gene_id_down,
        gene_name_down,
        chrom_down,
        pos_down,
        orientation_down,
        sv_id_down,

        junc_cn,
        cn,
        insert_seq,
        chain_length,
        transcripts_up,
        transcripts_down

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
                'gene_id_up',
                'gene_name_up',
                'chrom_up',
                'pos_up',
                'orientation_up',
                'sv_id_up',
                'gene_id_down',
                'gene_name_down',
                'chrom_down',
                'pos_down',
                'orientation_down',
                'sv_id_down',
                'junc_cn',
                'cn',
                'insert_seq',
                'chain_length',
                'transcripts_up',
                'transcripts_down'
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

        cast(gene_id_up as varchar) as gene_id_up,
        cast(gene_name_up as varchar) as gene_name_up,
        cast(chrom_up as varchar) as chrom_up,
        cast(pos_up as double) as pos_up,
        cast(orientation_up as double) as orientation_up,
        cast(sv_id_up as varchar) as sv_id_up,

        cast(gene_id_down as varchar) as gene_id_down,
        cast(gene_name_down as varchar) as gene_name_down,
        cast(chrom_down as varchar) as chrom_down,
        cast(pos_down as double) as pos_down,
        cast(orientation_down as double) as orientation_down,
        cast(sv_id_down as varchar) as sv_id_down,

        cast(junc_cn as double) as junc_cn,
        cast(cn as double) as cn,
        cast(insert_seq as varchar) as insert_seq,
        cast(chain_length as double) as chain_length,
        cast(transcripts_up as varchar) as transcripts_up,
        cast(transcripts_down as varchar) as transcripts_down

    from
        transformed

)

select * from final
