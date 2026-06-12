{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['workflow_run_hk', 'load_date', 'hash_diff'],
        merge_update_columns=['last_seen_datetime'],
        on_schema_change='fail',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
        partitioned_by=['load_date']
    )
}}

{% set source_table = 'metadata' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        input_dirs,
        output_dir,
        pkg_versions,
        files,

        json_format(cast(input_dirs as json)) as _input_dirs,
        json_format(cast(pkg_versions as json)) as _pkg_versions,
        json_format(cast(files as json)) as _files

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
        {{
            generate_hash_diff([
                'batch_id',
                'batch_date',
                '_input_dirs',
                'output_dir',
                '_pkg_versions',
                '_files'
            ])
        }} as hash_diff,
        *
    from
        source

),

transformed as (

    select
        cast('{{ run_started_at.strftime("%Y-%m-%d") }}' as date) as load_date,
        'tidywigits_{{ source_table }}' as record_source,
        cast('{{ run_started_at }}' as timestamp) as last_seen_datetime,
        *
    from
        encoded

),

final as (

    select
        cast(workflow_run_hk as varchar(64)) as workflow_run_hk,
        cast(load_date as date) as load_date,
        cast(hash_diff as varchar(64)) as hash_diff,
        cast(record_source as varchar(255)) as record_source,
        cast(last_seen_datetime as timestamp) as last_seen_datetime,
        cast(batch_id as varchar(26)) as batch_id,
        cast(batch_date as date) as batch_date,

        cast(input_dirs as array(varchar)) as input_dirs,
        cast(output_dir as varchar) as output_dir,
        cast(pkg_versions as array(row(name varchar, version varchar))) as pkg_versions,
        cast(files as array(row(tbl varchar, prefix varchar, fout varchar, fin varchar))) as files

    from
        transformed

)

select * from final
