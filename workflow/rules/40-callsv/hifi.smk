
rule sv_call_sniffles_hifi:
    input:
        bam = DIR_PROC.joinpath(
            "20-postalign", "{sample}_hifi.{aligner}.{ref}.sort.bam"),
        bai = DIR_PROC.joinpath(
            "20-postalign", "{sample}_hifi.{aligner}.{ref}.sort.bam.bai"),
        ref = lambda wildcards: REF_GENOMES[wildcards.ref],
        ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")],
    output:
        vcf = DIR_PROC.joinpath(
            "40-callsv", "{sample}_hifi.{aligner}-sniffles.{ref}.vcf"
        ),
        snf = DIR_PROC.joinpath(
            "40-callsv", "{sample}_hifi.{aligner}-sniffles.{ref}.snf"
        )
    log:
        DIR_LOG.joinpath("40-callsv", "{sample}_hifi.{aligner}-sniffles.{ref}.log")
    benchmark:
        DIR_RSRC.joinpath("40-callsv", "{sample}_hifi.{aligner}-sniffles.{ref}.rsrc")
    conda:
        DIR_ENVS.joinpath("caller", "sniffles.yaml")
    threads: CPU_MEDIUM
    resources:
        mem_mb=lambda wildcards, attempt: 32768 * attempt,
        time_hrs=lambda wildcards, attempt: f"{attempt**3}:59:59",
    params:
        min_sv_len = MIN_SV_LEN_CALL,
        min_mapq = MIN_MAPQ,
        min_cov = MIN_COV,
        min_aln_len = MIN_ALN_LEN
    shell:
        "sniffles --threads {threads} --no-progress --allow-overwrite "
        "--output-rnames "
        "--minsvlen {params.min_sv_len} "
        "--qc-coverage {params.min_cov} "
        "--mapq {params.min_mapq} "
        "--min-alignment-length {params.min_aln_len} "
        "--reference {input.ref} "
        "--input {input.bam} "
        "--snf {output.snf} "
        "--vcf {output.vcf} &> {log}"


rule sv_call_cutesv_hifi:
    """
    Important: the temporary working directory
    must exist before cuteSV starts!

    The first 4 runtime parameters (cluster bias/ratio merging)
    are the recommended default for HiFi (see tool help).

    """
    input:
        bam = DIR_PROC.joinpath(
            "20-postalign", "{sample}_hifi.{aligner}.{ref}.sort.bam"),
        bai = DIR_PROC.joinpath(
            "20-postalign", "{sample}_hifi.{aligner}.{ref}.sort.bam.bai"),
        ref = lambda wildcards: REF_GENOMES[wildcards.ref],
        ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")],
    output:
        vcf = DIR_PROC.joinpath(
            "40-callsv", "{sample}_hifi.{aligner}-cutesv.{ref}.vcf"
        ),
    log:
        DIR_LOG.joinpath("40-callsv", "{sample}_hifi.{aligner}-cutesv.{ref}.log")
    benchmark:
        DIR_RSRC.joinpath("40-callsv", "{sample}_hifi.{aligner}-cutesv.{ref}.rsrc")
    conda:
        DIR_ENVS.joinpath("caller", "cutesv.yaml")
    threads: CPU_LOW
    resources:
        mem_mb=lambda wildcards, attempt: 32768 * attempt,
        time_hrs=lambda wildcards, attempt: f"{attempt*attempt}:59:59",
    params:
        min_sv_len = MIN_SV_LEN_CALL,
        min_mapq = MIN_MAPQ,
        min_cov = MIN_COV,
        min_aln_len = MIN_ALN_LEN,
        tmp_wd = lambda wildcards, output: pathlib.Path(output.vcf).with_suffix(".wd.tmp")
    shell:
        "rm -rfd {params.tmp_wd} && mkdir -p {params.tmp_wd} "
            " && "
        "cuteSV -t {threads} -S {wildcards.sample} "
        "--report_readid "
        "--max_cluster_bias_INS 1000 "
        "--diff_ratio_merging_INS 0.9 "
        "--max_cluster_bias_DEL 1000 "
        "--diff_ratio_merging_DEL 0.5 "
        "--min_size {params.min_sv_len} "
        "--max_size -1 "
        "--min_mapq {params.min_mapq} "
        "--min_read_len {params.min_aln_len} "
        "--min_support {params.min_cov} "
        "{input.bam} {input.reference} {output.vcf} {params.out_dir} &> {log}"
        " ; rm -rfd {params.tmp_wd}"


rule run_sniffles_hifi_sv_calling:
    input:
        vcf = expand(
            DIR_PROC.joinpath(
                "40-callsv", "{sample}_hifi.{aligner}-sniffles.{ref}.vcf"
            ),
            sample=HIFI_SAMPLES,
            aligner=ALIGNER_FOR_CALLER[("sniffles", "hifi")],
            ref=USE_REF_GENOMES
        )


rule run_cutesv_hifi_sv_calling:
    input:
        vcf = expand(
            DIR_PROC.joinpath(
                "40-callsv", "{sample}_hifi.{aligner}-cutesv.{ref}.vcf"
            ),
            sample=HIFI_SAMPLES,
            aligner=ALIGNER_FOR_CALLER[("cutesv", "hifi")],
            ref=USE_REF_GENOMES
        )


rule run_hifi_sv_calling:
    input:
        vcf = expand(
            DIR_PROC.joinpath(
                "40-callsv", "{sample}_hifi.{sv_calling_toolchain}.{ref}.vcf"
            ),
            sample=HIFI_SAMPLES,
            hifi_sv_call_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
            ref=USE_REF_GENOMES
        )
