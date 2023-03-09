
rule merge_alignments_per_sample:
    """
    This creates disk space overhead by copying
    the data for samples that were sequenced
    with only a single cell - too bad ...
    """
    input:
        bams = lambda wildcards: expand(
            DIR_PROC.joinpath(
                "10-align", "{{sample}}_{{read_type}}_{path_id}.{{aligner}}.{{ref}}.sort.bam",
                path_id=MAP_SAMPLE_TO_INPUT_FILES[wildcards.sample][wildcards.read_type]["path_ids"]
            ),
        ),
        bais = lambda wildcards: expand(
            DIR_PROC.joinpath(
                "10-align", "{{sample}}_{{read_type}}_{path_id}.{{aligner}}.{{ref}}.sort.bam.bai",
                path_id=MAP_SAMPLE_TO_INPUT_FILES[wildcards.sample][wildcards.read_type]["path_ids"]
            ),
        ),
    output:
        bam = DIR_PROC.joinpath(
            "20-postalign", "{sample}_{read_type}.{aligner}.{ref}.sort.bam"),
        bai = DIR_PROC.joinpath(
            "20-postalign", "{sample}_{read_type}.{aligner}.{ref}.sort.bam.bai"),
    benchmark:
        DIR_RSRC.joinpath(
            "20-postalign", "{sample}_{read_type}.{aligner}.{ref}.sort.samtools-merge.rsrc",
        )
    log:
        DIR_LOG.joinpath(
            "20-postalign", "{sample}_{read_type}.{aligner}.{ref}.sort.samtools-merge.log",
        )
    conda:
        "../../envs/biotools.yaml"
    resources:
        mem_mb = lambda wildcards, attempt: 2048 * attempt,
        time_hrs = lambda wildcards, attempt: 1 * attempt,
    shell:
        "samtools merge -f -p {output.bam} {input.bams} &> {log}"
            " && "
        "samtools index {output.bam}"
