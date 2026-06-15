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

{#
  This is the Business Vault layer satellite. The model unnest
  the structure into `pkg_name`, `pkg_version`, `library_id` and
  along with aggregated `batch_file_count` and `batch_table_count`.

  The satellite is also hooked onto Link model between two Hubs
  Library and WorkflowRun. This enables finest possible data grain
  for data mart table lookup later.
#}

with source as (

    select
        workflow_run_hk,
        batch_id,
        batch_date,
        coalesce(cardinality(files), 0) as batch_file_count,
        cardinality(array_distinct(transform(files, f -> f.tbl))) as batch_table_count,
        exploded_pkg.name as pkg_name,
        exploded_pkg.version as pkg_version,
        exploded_prefix as library_id
    from
        {{ ref('sat_tidywigits_metadata_detail') }}
    left join
        unnest(pkg_versions) as t(exploded_pkg) on true
    left join
        unnest(array_distinct(transform(files, f -> f.prefix))) as t2(exploded_prefix) on true

    {% if is_incremental() %}

    where
        batch_date >=( select max(load_date) from {{ this }} )

    {% endif %}

),

encoded as (

    select
        lower(to_hex(sha256(cast(library_id as varbinary)))) as library_hk,
        {{
            generate_hash_diff([
                'batch_id',
                'batch_date',
                'batch_file_count',
                'batch_table_count',
                'pkg_name',
                'pkg_version'
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
        'sat_tidywigits_metadata_detail' as record_source,
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
        cast(batch_file_count as bigint) as batch_file_count,
        cast(batch_table_count as bigint) as batch_table_count,
        cast(pkg_name as varchar(255)) as pkg_name,
        cast(pkg_version as varchar(255)) as pkg_version
    from
        transformed

)

select * from final
