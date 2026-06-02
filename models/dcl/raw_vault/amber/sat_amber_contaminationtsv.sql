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

{% set source_table = 'amber_contaminationtsv' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_pfix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        chrom,
        pos,
        "ref",
        alt,
        dp_normal,
        refsup_normal,
        altsup_normal,
        dp_tumor,
        refsup_tumor,
        altsup_tumor

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
                'pos',
                '"ref"',
                'alt',
                'dp_normal',
                'refsup_normal',
                'altsup_normal',
                'dp_tumor',
                'refsup_tumor',
                'altsup_tumor'
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
        cast(pos as bigint) as pos,
        cast("ref" as varchar) as "ref",
        cast(alt as varchar) as alt,
        cast(dp_normal as bigint) as dp_normal,
        cast(refsup_normal as bigint) as refsup_normal,
        cast(altsup_normal as bigint) as altsup_normal,
        cast(dp_tumor as bigint) as dp_tumor,
        cast(refsup_tumor as bigint) as refsup_tumor,
        cast(altsup_tumor as bigint) as altsup_tumor

    from
        transformed

)

select * from final
