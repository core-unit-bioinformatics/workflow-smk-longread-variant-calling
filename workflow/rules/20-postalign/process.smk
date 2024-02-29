
rule merge_alignments_per_sample:
    """
    This creates disk space overhead by copying
    the data for samples that were sequenced
    with only a single cell - too bad ...
    """
    input:
        bams = lambda wildcards: expand(
            DIR_PROC.joinpath(
                "10-align", "{{sample}}_{{read_type}}_{path_id}.{{aligner}}.{{ref}}.sort.bam"
            ),
            path_id=MAP_SAMPLE_TO_INPUT_FILES[wildcards.sample][wildcards.read_type]["path_ids"]
        ),
        bais = lambda wildcards: expand(
            DIR_PROC.joinpath(
                "10-align", "{{sample}}_{{read_type}}_{path_id}.{{aligner}}.{{ref}}.sort.bam.bai"
            ),
            path_id=MAP_SAMPLE_TO_INPUT_FILES[wildcards.sample][wildcards.read_type]["path_ids"]
        ),
    output:
        bam = DIR_PROC.joinpath(
            "20-postalign", "merge", "{sample}_{read_type}.{aligner}.{ref}.sort.bam"),
        bai = DIR_PROC.joinpath(
            "20-postalign", "merge", "{sample}_{read_type}.{aligner}.{ref}.sort.bam.bai"),
    benchmark:
        DIR_RSRC.joinpath(
            "20-postalign", "merge", "{sample}_{read_type}.{aligner}.{ref}.sort.samtools-merge.rsrc",
        )
    log:
        DIR_LOG.joinpath(
            "20-postalign", "merge", "{sample}_{read_type}.{aligner}.{ref}.sort.samtools-merge.log",
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb = lambda wildcards, attempt: 2048 * attempt,
        time_hrs = lambda wildcards, attempt: 1 * attempt,
    shell:
        "samtools merge -@ {threads} -f -p {output.bam} {input.bams} &> {log}"
            " && "
        "samtools index {output.bam}"


rule split_merged_alignments:
    input:
        bam = rules.merge_alignments_per_sample.output.bam,
        bai = rules.merge_alignments_per_sample.output.bam
    output:
        main = DIR_PROC.joinpath(
            "20-postalign", "split", "{sample}_{read_type}.{aligner}.{ref}.main.sort.bam"
        ),
        main_bai = DIR_PROC.joinpath(
            "20-postalign", "split", "{sample}_{read_type}.{aligner}.{ref}.main.sort.bam.bai"
        ),
        aux =  DIR_PROC.joinpath(
            "20-postalign", "split", "{sample}_{read_type}.{aligner}.{ref}.aux.sort.bam"
        ),
        aux_bai =  DIR_PROC.joinpath(
            "20-postalign", "split", "{sample}_{read_type}.{aligner}.{ref}.aux.sort.bam.bai"
        )
    log:
        DIR_LOG.joinpath(
            "20-postalign", "split", "{sample}_{read_type}.{aligner}.{ref}.sort.samtools-split.log"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb = lambda wildcards, attempt: 2048 * attempt,
        time_hrs = lambda wildcards, attempt: 1 * attempt,
    params:
        aux_flag=SAM_FLAG_SPLIT
    shell:
        "samtools view -F {params.aux_flag} -b -o {output.main} "
        "--output-unselected {output.aux} {input.bam} &> {log}"
            " && "
        "samtools index {output.main}"
            " && "
        "samtools index {output.aux}"


rule compute_alignment_flagstats:
    input:
        bam = DIR_PROC.joinpath(
            "20-postalign", "split", "{sample}_{read_type}.{aligner}.{ref}.{bam_type}.sort.bam"
        ),
        bai = DIR_PROC.joinpath(
            "20-postalign", "split", "{sample}_{read_type}.{aligner}.{ref}.{bam_type}.sort.bam.bai"
        )
    output:
        stats = DIR_RES.joinpath(
            "statistics", "aln_flagstats", "{sample}_{read_type}.{aligner}.{ref}.{bam_type}.flagstats.txt"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    shell:
        "samtools flagstats {input.bam} > {output.stats}"


rule run_all_hifi_align:
    input:
        bams_main = expand(
            rules.split_merged_alignments.output.main,
            read_type=["hifi"],
            ref=USE_REF_GENOMES,
            sample=HIFI_SAMPLES,
            aligner=HIFI_ALIGNER_WILDCARDS
        ),
        bams_aux = expand(
            rules.split_merged_alignments.output.aux,
            read_type=["hifi"],
            ref=USE_REF_GENOMES,
            sample=HIFI_SAMPLES,
            aligner=HIFI_ALIGNER_WILDCARDS
        ),
        stats = expand(
            rules.compute_alignment_flagstats.output.stats,
            ref=USE_REF_GENOMES,
            sample=HIFI_SAMPLES,
            read_type=["hifi"],
            aligner=HIFI_ALIGNER_WILDCARDS,
            bam_type=["main", "aux"]
        ),
