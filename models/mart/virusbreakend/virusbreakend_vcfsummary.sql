{{
    config(
        on_schema_change='fail',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
        partitioned_by=['portal_run_date']
    )
}}

with transformed as (

    select
        wfl.portal_run_id,
        cast(parse_datetime(substr(portal_run_id, 1, 8), 'yyyymmdd') as date) as portal_run_date,
        lib.library_id,
        sat.taxid_genus,
        sat.name_genus,
        sat.reads_genus_tree,
        sat.taxid_species,
        sat.name_species,
        sat.reads_species_tree,
        sat.taxid_assigned,
        sat.name_assigned,
        sat.reads_assigned_tree,
        sat.reads_assigned_direct,
        sat.reference,
        sat.reference_taxid,
        sat.reference_kmer_count,
        sat.alternate_kmer_count,
        sat.rname,
        sat.startpos,
        sat.endpos,
        sat.numreads,
        sat.covbases,
        sat.coverage,
        sat.meandepth,
        sat.meanbaseq,
        sat.meanmapq,
        sat.integrations,
        sat.qc_status
    from {{ ref('hub_workflow_run') }} wfl
        join {{ ref('link_library_workflow_run') }} lnk on lnk.workflow_run_hk = wfl.workflow_run_hk
        join {{ ref('hub_library') }} lib on lib.library_hk = lnk.library_hk
        join {{ ref('sat_virusbreakend_vcfsummary') }} sat on sat.library_workflow_run_hk = lnk.library_workflow_run_hk

),

final as (

    select
        cast(portal_run_id as varchar(16)) as portal_run_id,
        cast(portal_run_date as date) as portal_run_date,
        cast(library_id as varchar(64)) as library_id,

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

