
rule sv_call_sniffles_hifi:
    """TODO check
    based on this statement
    https://github.com/fritzsedlazeck/Sniffles/issues/123#issuecomment-460705150
    the "-s" parameter determines
    the read support threshold for a call;
    Since there is no "-s" parameter in current version,
    that probably refers to the parameter
    "--minsupport [default: auto]"
    Check if that makes a difference
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
        time_hrs=lambda wildcards, attempt: attempt**3,
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
        time_hrs=lambda wildcards, attempt: attempt*attempt,
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
        "--genotype "
        "--max_cluster_bias_INS 1000 "
        "--diff_ratio_merging_INS 0.9 "
        "--max_cluster_bias_DEL 1000 "
        "--diff_ratio_merging_DEL 0.5 "
        "--min_size {params.min_sv_len} "
        "--min_mapq {params.min_mapq} "
        "--min_read_len {params.min_aln_len} "
        "--min_support {params.min_cov} "
        "{input.bam} {input.ref} {output.vcf} {params.tmp_wd} &> {log}"
        " ; rm -rfd {params.tmp_wd}"


rule sv_discover_pbsv_hifi:
    """
    In exceptional cases, pbsv discover requires a comparatively large
    amount of memory (~3 times of avg.)
    """
    input:
        bam = DIR_PROC.joinpath(
            "20-postalign", "{sample}_hifi.{aligner}.{ref}.sort.bam"),
        bai = DIR_PROC.joinpath(
            "20-postalign", "{sample}_hifi.{aligner}.{ref}.sort.bam.bai"),
    output:
        svsig = temp(DIR_PROC.joinpath(
            "40-callsv", "{sample}_hifi.{aligner}-pbsv.{ref}.{chrom}.svsig.gz"
        )),

    log:
        DIR_LOG.joinpath(
            "40-callsv", "{sample}_hifi.{aligner}-pbsv.{ref}.{chrom}.discover.log"
        ),
    benchmark:
        DIR_RSRC.joinpath(
            "40-callsv", "{sample}_hifi.{aligner}-pbsv.{ref}.{chrom}.discover.rsrc"
        ),
    conda:
        DIR_ENVS.joinpath("caller", "pbsv.yaml")
    resources:
        mem_mb = lambda wildcards, attempt: 2048 + 2048 * attempt * attempt,
        time_hrs = lambda wildcards, attempt: attempt
    params:
        min_sv_len = MIN_SV_LEN_CALL,
        min_mapq = MIN_MAPQ,
    shell:
        'pbsv discover --hifi --region {wildcards.chrom} '
        '--min-mapq {params.min_mapq} '
        '--min-svsig-length {params.min_sv_len} '
        '{input.bam} {output.svsig} &> {log}'


rule sv_call_pbsv_hifi:
    input:
        ref = lambda wildcards: REF_GENOMES[wildcards.ref],
        ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")],

        svsig = expand(DIR_PROC.joinpath(
            "40-callsv", "{{sample}}_hifi.{{aligner}}-pbsv.{{ref}}.{chrom}.svsig.gz"),
            chrom=CHROMOSOMES
        )
    output:
        vcf = DIR_PROC.joinpath("40-callsv", "{sample}_hifi.{aligner}-pbsv.{ref}.vcf"),
    log:
        DIR_LOG.joinpath("40-callsv", "{sample}_hifi.{aligner}-pbsv.{ref}.call.log"),
    benchmark:
        DIR_RSRC.joinpath("40-callsv", "{sample}_hifi.{aligner}-pbsv.{ref}.call.rsrc"),
    conda:
        DIR_ENVS.joinpath("caller", "pbsv.yaml")
    threads: CPU_MEDIUM
    resources:
        mem_mb = lambda wildcards, attempt: 16384 + 8192 * attempt,
        time_hrs = lambda wildcards, attempt: attempt
    params:
        min_sv_len = MIN_SV_LEN_CALL,
    shell:
        'pbsv call -j {threads} --hifi '
        '--min-sv-length {params.min_sv_len} '
        '{input.ref} {input.svsig} {output.vcf} &> {log}'


rule run_pbsv_hifi_sv_calling:
    input:
        vcf = expand(
            DIR_PROC.joinpath(
                "40-callsv", "{sample}_hifi.{aligner}-pbsv.{ref}.vcf"
            ),
            sample=HIFI_SAMPLES,
            aligner=ALIGNER_FOR_CALLER[("pbsv", "hifi")],
            ref=USE_REF_GENOMES
        )


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
            sv_calling_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
            ref=USE_REF_GENOMES
        )
