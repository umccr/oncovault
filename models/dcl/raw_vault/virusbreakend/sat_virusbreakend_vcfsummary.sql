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

{% set source_table = 'virusbreakend_vcfsummary' %}

with source as (

    select
        trim(regexp_replace(input_id, '[\n\r]+', '')) as portal_run_id,
        trim(regexp_replace(input_pfix, '[\n\r]+', '')) as library_id,
        trim(regexp_replace(output_id, '[\n\r]+', '')) as batch_id,
        batch_date,

        taxid_genus,
        name_genus,
        reads_genus_tree,
        taxid_species,
        name_species,
        reads_species_tree,
        taxid_assigned,
        name_assigned,
        reads_assigned_tree,
        reads_assigned_direct,
        reference,
        reference_taxid,
        reference_kmer_count,
        alternate_kmer_count,
        rname,
        startpos,
        endpos,
        numreads,
        covbases,
        coverage,
        meandepth,
        meanbaseq,
        meanmapq,
        integrations,
        qc_status

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
                'taxid_genus',
                'name_genus',
                'reads_genus_tree',
                'taxid_species',
                'name_species',
                'reads_species_tree',
                'taxid_assigned',
                'name_assigned',
                'reads_assigned_tree',
                'reads_assigned_direct',
                'reference',
                'reference_taxid',
                'reference_kmer_count',
                'alternate_kmer_count',
                'rname',
                'startpos',
                'endpos',
                'numreads',
                'covbases',
                'coverage',
                'meandepth',
                'meanbaseq',
                'meanmapq',
                'integrations',
                'qc_status'
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

        cast(taxid_genus as varchar) as taxid_genus,
        cast(name_genus as varchar) as name_genus,
        cast(reads_genus_tree as bigint) as reads_genus_tree,
        cast(taxid_species as varchar) as taxid_species,
        cast(name_species as varchar) as name_species,
        cast(reads_species_tree as bigint) as reads_species_tree,
        cast(taxid_assigned as varchar) as taxid_assigned,
        cast(name_assigned as varchar) as name_assigned,
        cast(reads_assigned_tree as bigint) as reads_assigned_tree,
        cast(reads_assigned_direct as bigint) as reads_assigned_direct,
        cast(reference as varchar) as reference,
        cast(reference_taxid as varchar) as reference_taxid,
        cast(reference_kmer_count as bigint) as reference_kmer_count,
        cast(alternate_kmer_count as bigint) as alternate_kmer_count,
        cast(rname as varchar) as rname,
        cast(startpos as bigint) as startpos,
        cast(endpos as bigint) as endpos,
        cast(numreads as bigint) as numreads,
        cast(covbases as bigint) as covbases,
        cast(coverage as double) as coverage,
        cast(meandepth as double) as meandepth,
        cast(meanbaseq as double) as meanbaseq,
        cast(meanmapq as double) as meanmapq,
        cast(integrations as double) as integrations,
        cast(qc_status as varchar) as qc_status

    from
        transformed

)

select * from final
