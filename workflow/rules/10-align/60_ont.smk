rule align_minimap2_ont:
    """
    -a: output in SAM
    --eqx: write =/X CIGAR operators
    -Y: use soft-clipping for suppl. alns
    --MD: output MD tag
    (SAMspecs: String encoding mismatched and deleted reference bases)
    -N 1: keep at most 1 secondary alignment
    -p 0.8: min secondary-to-primary score ratio
    -x map-hifi: HiFi preset
    """
    input:
        reads = lambda wildcards: MAP_PATHID_TO_FILE_INFO[wildcards.path_id]["path"],
        reference = lambda wildcards: load_reference_genome(wildcards)
        #reference = lambda wildcards: REF_GENOMES[wildcards.ref]
    output:
        sort = temp(DIR_PROC.joinpath(
            "10-align", "{sample}_ont_{path_id}.mm2.{ref}.sort.bam"
        )),
        index = temp(DIR_PROC.joinpath(
            "10-align", "{sample}_ont_{path_id}.mm2.{ref}.sort.bam.bai"
        )),
        exclude = DIR_PROC.joinpath(
            "10-align", "{sample}_ont_{path_id}.mm2.{ref}.excl.bam"
        ),
    benchmark:
        DIR_RSRC.joinpath(
            "10-align", "{sample}_ont_{path_id}.mm2.{ref}.rsrc"
        )
    log:
        aln = DIR_LOG.joinpath(
            "10-align", "{sample}_ont_{path_id}.mm2.{ref}.align.log"
        ),
        sam = DIR_LOG.joinpath(
            "10-align", "{sample}_ont_{path_id}.mm2.{ref}.samtools.log"
        ),

    conda:
        DIR_ENVS.joinpath("aligner", "mm2.yaml")
    threads: CPU_MEDIUM
    resources:
        mem_mb = lambda wildcards, input, attempt: input.size_mb + 16384 * attempt,
        time_hrs = lambda wildcards, attempt: 2 + 2 * attempt,
        sort_mem_mb = lambda wildcards, attempt: 1024 * attempt
    params:
        readgroup = lambda wildcards: (
            f'"@RG\\tID:{wildcards.sample}_{wildcards.path_id}'
            f'\\tSM:{wildcards.sample}"'
        ),
        sam_flag_out = SAM_FLAG_DISCARD,
        sam_threads = CPU_LOW,
        aln_thres = MIN_RATIO_PRIME_TO_SECOND,
        keep_n_second = KEEP_AT_MOST_N_SECOND,
        acc_in=lambda wildcards, input: register_input(input.reads),
        acc_ref=lambda wildcards, input: register_reference(input.reference),
    shell:
        "minimap2 -a -x map-ont -p {params.aln_thres} -Y --MD --eqx -L -t {threads} "
        " -R {params.readgroup} -N {params.keep_n_second} {input.reference} "
        " {input.reads} 2> {log.aln}"
            " | "
        " samtools view -u -h --output-unselected {output.exclude} "
        " -F {params.sam_flag_out} --threads {params.sam_threads}"
            " | "
        " samtools sort -l 9 -m {resources.sort_mem_mb}M "
        " --threads {params.sam_threads} "
        " -T {wildcards.sample}_{wildcards.path_id}_{wildcards.ref}_mm2 -o {output.sort} "
        " 2> {log.sam}"
            " && "
        "samtools index -@ {threads} {output.sort}"


rule run_minimap2_ont_align:
    input:
        bams = expand(
            rules.align_minimap2_ont.output.sort,
            expand_ont_reads,
            ref=USE_REF_GENOMES,
            sample=ONT_SAMPLES,
            path_id=ONT_INPUT,
        )
