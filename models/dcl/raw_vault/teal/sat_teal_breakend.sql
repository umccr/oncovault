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

{% set source_table = 'teal_breakend' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_pfix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        chrom,
        position,
        orientation,
        cg_rich,
        filter,
        in_tumor,
        in_germline,
        distance_to_telomere,
        max_telomeric_length,
        max_anchor_length,
        tumor_sr_tel_dp_tel,
        tumor_sr_tel_dp_no_tel,
        tumor_sr_tel_no_dp,
        tumor_sr_no_tel_dp_tel,
        tumor_dp_tel_no_sr,
        tumor_total_support,
        tumor_mapq,
        germ_sr_tel_dp_tel,
        germ_sr_tel_dp_no_tel,
        germ_sr_tel_no_dp,
        germ_sr_no_tel_dp_tel,
        germ_dp_tel_no_sr,
        germ_total_support,
        germ_mapq

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
                'chrom',
                'position',
                'orientation',
                'cg_rich',
                'filter',
                'in_tumor',
                'in_germline',
                'distance_to_telomere',
                'max_telomeric_length',
                'max_anchor_length',
                'tumor_sr_tel_dp_tel',
                'tumor_sr_tel_dp_no_tel',
                'tumor_sr_tel_no_dp',
                'tumor_sr_no_tel_dp_tel',
                'tumor_dp_tel_no_sr',
                'tumor_total_support',
                'tumor_mapq',
                'germ_sr_tel_dp_tel',
                'germ_sr_tel_dp_no_tel',
                'germ_sr_tel_no_dp',
                'germ_sr_no_tel_dp_tel',
                'germ_dp_tel_no_sr',
                'germ_total_support',
                'germ_mapq'
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

        cast(chrom as varchar) as chrom,
        cast("position" as double) as "position",
        cast(orientation as double) as orientation,
        cast(cg_rich as varchar) as cg_rich,
        cast(filter as varchar) as filter,
        cast(in_tumor as varchar) as in_tumor,
        cast(in_germline as varchar) as in_germline,
        cast(distance_to_telomere as double) as distance_to_telomere,
        cast(max_telomeric_length as double) as max_telomeric_length,
        cast(max_anchor_length as double) as max_anchor_length,
        cast(tumor_sr_tel_dp_tel as double) as tumor_sr_tel_dp_tel,
        cast(tumor_sr_tel_dp_no_tel as double) as tumor_sr_tel_dp_no_tel,
        cast(tumor_sr_tel_no_dp as double) as tumor_sr_tel_no_dp,
        cast(tumor_sr_no_tel_dp_tel as double) as tumor_sr_no_tel_dp_tel,
        cast(tumor_dp_tel_no_sr as double) as tumor_dp_tel_no_sr,
        cast(tumor_total_support as double) as tumor_total_support,
        cast(tumor_mapq as double) as tumor_mapq,
        cast(germ_sr_tel_dp_tel as double) as germ_sr_tel_dp_tel,
        cast(germ_sr_tel_dp_no_tel as double) as germ_sr_tel_dp_no_tel,
        cast(germ_sr_tel_no_dp as double) as germ_sr_tel_no_dp,
        cast(germ_sr_no_tel_dp_tel as double) as germ_sr_no_tel_dp_tel,
        cast(germ_dp_tel_no_sr as double) as germ_dp_tel_no_sr,
        cast(germ_total_support as double) as germ_total_support,
        cast(germ_mapq as double) as germ_mapq

    from
        transformed

)

select * from final
