
rule sv_call_sniffles_hifi:
    input:
        bam = expand(
            rules.split_merged_alignments.output.main,
            read_type="hifi",
            allow_missing=True
        ),
        bai = expand(
            rules.split_merged_alignments.output.main_bai,
            read_type="hifi",
            allow_missing=True
        ),
        ref = lambda wildcards: load_reference_genome(wildcards),
        ref_idx = lambda wildcards: load_reference_genome(wildcards, index_file=True)
        # ref = lambda wildcards: REF_GENOMES[wildcards.ref],
        # ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")],
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
        min_mapq = lambda wildcards: load_min_mapq_threshold(wildcards),
        min_cov = MIN_COV,
        min_aln_len = MIN_ALN_LEN
    shell:
        "sniffles --threads {threads} --no-progress --allow-overwrite "
        "--output-rnames --sample-id {wildcards.sample} "
        "--minsvlen {params.min_sv_len} "
        "--minsupport {params.min_cov} "
        "--qc-coverage {params.min_cov} "
        "--mapq {params.min_mapq} "
        "--min-alignment-length {params.min_aln_len} "
        "--reference {input.ref} "
        "--input {input.bam} "
        "--snf {output.snf} "
        "--vcf {output.vcf} &> {log}"


rule sv_call_sniffles_hifi_all_samples:
    input:
        snf = expand(
            rules.sv_call_sniffles_hifi.output.snf,
            sample=HIFI_SAMPLES,
            allow_missing=True
        ),
        ref = lambda wildcards: load_reference_genome(wildcards),
        ref_idx = lambda wildcards: load_reference_genome(wildcards, index_file=True)
        # ref = lambda wildcards: REF_GENOMES[wildcards.ref],
        # ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")],
    output:
        vcf = DIR_PROC.joinpath(
            "40-callsv", "SAMPLES_hifi.{aligner}-sniffles.{ref}.vcf"
        )
    log:
        DIR_LOG.joinpath("40-callsv", "SAMPLES_hifi.{aligner}-sniffles.{ref}.log")
    benchmark:
        DIR_RSRC.joinpath("40-callsv", "SAMPLES_hifi.{aligner}-sniffles.{ref}.rsrc")
    conda:
        DIR_ENVS.joinpath("caller", "sniffles.yaml")
    threads: CPU_MEDIUM
    resources:
        mem_mb=lambda wildcards, attempt: 32768 * attempt,
        time_hrs=lambda wildcards, attempt: attempt**3,
    params:
        min_sv_len = MIN_SV_LEN_CALL,
        min_mapq = lambda wildcards: load_min_mapq_threshold(wildcards),
        min_cov = MIN_COV,
        min_aln_len = MIN_ALN_LEN
    shell:
        "sniffles --threads {threads} --no-progress --allow-overwrite "
        "--output-rnames "
        "--minsvlen {params.min_sv_len} "
        "--minsupport {params.min_cov} "
        "--combine-null-min-coverage {params.min_cov} "
        "--qc-coverage {params.min_cov} "
        "--mapq {params.min_mapq} "
        "--min-alignment-length {params.min_aln_len} "
        "--reference {input.ref} "
        "--input {input.snf} "
        "--vcf {output.vcf} &> {log}"


rule sv_call_sniffles_mosaic_hifi:
    """
    """
    input:
        bam = expand(
            rules.split_merged_alignments.output.main,
            read_type="hifi",
            allow_missing=True
        ),
        bai = expand(
            rules.split_merged_alignments.output.main_bai,
            read_type="hifi",
            allow_missing=True
        ),
        ref = lambda wildcards: load_reference_genome(wildcards),
        ref_idx = lambda wildcards: load_reference_genome(wildcards, index_file=True)
        # ref = lambda wildcards: REF_GENOMES[wildcards.ref],
        # ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")],
    output:
        vcf = DIR_PROC.joinpath(
            "40-callsv", "{sample}_hifi.{aligner}-sniffles.mosaic.{ref}.vcf"
        ),
        snf = DIR_PROC.joinpath(
            "40-callsv", "{sample}_hifi.{aligner}-sniffles.mosaic.{ref}.snf"
        )
    log:
        DIR_LOG.joinpath("40-callsv", "{sample}_hifi.{aligner}-sniffles.mosaic.{ref}.log")
    benchmark:
        DIR_RSRC.joinpath("40-callsv", "{sample}_hifi.{aligner}-sniffles.mosaic.{ref}.rsrc")
    wildcard_constraints:
        ref=CONSTRAINT_REF_GENOMES
    conda:
        DIR_ENVS.joinpath("caller", "sniffles.yaml")
    threads: CPU_MEDIUM
    resources:
        mem_mb=lambda wildcards, attempt: 32768 * attempt,
        time_hrs=lambda wildcards, attempt: attempt**3,
    params:
        min_sv_len = MIN_SV_LEN_CALL,
        min_mapq = lambda wildcards: load_min_mapq_threshold(wildcards),
        min_cov = MIN_COV,
        min_aln_len = MIN_ALN_LEN
    shell:
        "sniffles --threads {threads} --no-progress --allow-overwrite "
        "--output-rnames --mosaic --sample-id {wildcards.sample} "
        "--minsvlen {params.min_sv_len} "
        "--qc-coverage {params.min_cov} "
        "--mapq {params.min_mapq} "
        "--min-alignment-length {params.min_aln_len} "
        "--reference {input.ref} "
        "--input {input.bam} "
        "--snf {output.snf} "
        "--vcf {output.vcf} &> {log}"


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


rule run_sniffles_hifi_sv_calling_mosaic:
    input:
        vcf = expand(
            DIR_PROC.joinpath(
                "40-callsv", "{sample}_hifi.{aligner}-sniffles.mosaic.{ref}.vcf"
            ),
            sample=HIFI_SAMPLES,
            aligner=ALIGNER_FOR_CALLER[("sniffles", "hifi")],
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


if SAMPLE_PAIRS is not None:
    rule run_hifi_sv_calling_personalized:
        input:
            vcf = expand(
                DIR_PROC.joinpath(
                    "40-callsv", "{sample}_hifi.{sv_calling_toolchain}.{ref}.vcf"
                ),
                sample=HIFI_SAMPLES,
                sv_calling_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
                ref=["prg", "prg1", "prg2"]
            )


if RUN_SNIFFLES_MULTISAMPLE_MODE:
    rule run_sniffles_hifi_sv_calling_multisample:
        input:
            vcf = expand(
                rules.sv_call_sniffles_hifi_all_samples.output.vcf,
                ref=USE_REF_GENOMES,
                aligner=ALIGNER_FOR_CALLER[("sniffles", "hifi")]
            )
