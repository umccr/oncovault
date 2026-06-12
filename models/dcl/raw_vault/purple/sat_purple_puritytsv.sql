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

{% set source_table = 'purple_puritytsv' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_prefix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        purity,
        norm_factor,
        fit_score,
        diploid_proportion,
        ploidy,
        gender,
        status,
        polyclonal_proportion,
        purity_min,
        purity_max,
        ploidy_min,
        ploidy_max,
        diploid_proportion_min,
        diploid_proportion_max,
        somatic_penalty,
        whole_genome_duplication,
        ms_indels_per_mb,
        ms_status,
        tml,
        tml_status,
        tmb_per_mb,
        tmb_status,
        tmb_sv,
        run_mode,
        targeted

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
                'purity',
                'norm_factor',
                'fit_score',
                'diploid_proportion',
                'ploidy',
                'gender',
                'status',
                'polyclonal_proportion',
                'purity_min',
                'purity_max',
                'ploidy_min',
                'ploidy_max',
                'diploid_proportion_min',
                'diploid_proportion_max',
                'somatic_penalty',
                'whole_genome_duplication',
                'ms_indels_per_mb',
                'ms_status',
                'tml',
                'tml_status',
                'tmb_per_mb',
                'tmb_status',
                'tmb_sv',
                'run_mode',
                'targeted'
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

        cast(purity as double) as purity,
        cast(norm_factor as double) as norm_factor,
        cast(fit_score as double) as fit_score,
        cast(diploid_proportion as double) as diploid_proportion,
        cast(ploidy as double) as ploidy,
        cast(gender as varchar) as gender,
        cast(status as varchar) as status,
        cast(polyclonal_proportion as double) as polyclonal_proportion,
        cast(purity_min as double) as purity_min,
        cast(purity_max as double) as purity_max,
        cast(ploidy_min as double) as ploidy_min,
        cast(ploidy_max as double) as ploidy_max,
        cast(diploid_proportion_min as double) as diploid_proportion_min,
        cast(diploid_proportion_max as double) as diploid_proportion_max,
        cast(somatic_penalty as double) as somatic_penalty,
        cast(whole_genome_duplication as varchar) as whole_genome_duplication,
        cast(ms_indels_per_mb as double) as ms_indels_per_mb,
        cast(ms_status as varchar) as ms_status,
        cast(tml as double) as tml,
        cast(tml_status as varchar) as tml_status,
        cast(tmb_per_mb as double) as tmb_per_mb,
        cast(tmb_status as varchar) as tmb_status,
        cast(tmb_sv as double) as tmb_sv,
        cast(run_mode as varchar) as run_mode,
        cast(targeted as varchar) as targeted

    from
        transformed

)

select * from final
