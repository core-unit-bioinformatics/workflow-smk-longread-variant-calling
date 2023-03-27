
rule short_call_deepvariant_hifi:
    input:
        ref = lambda wildcards: REF_GENOMES[wildcards.ref],
        ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")],
        bam = DIR_PROC.joinpath(
            "20-postalign", "{sample}_{read_type}.{aligner}.{ref}.sort.bam"),
        bai = DIR_PROC.joinpath(
            "20-postalign", "{sample}_{read_type}.{aligner}.{ref}.sort.bam.bai"),
    output:
        vcfgz = DIR_PROC.joinpath(
            "30-callshort", "{sample}_{read_type}.{aligner}-deepvar.{ref}.{chrom}.vcf.gz"
        )
    benchmark:
        DIR_RSRC.joinpath(
            "30-callshort", "{sample}_{read_type}.{aligner}-deepvar.{ref}.{chrom}.rsrc"
        )
    log:
        DIR_LOG.joinpath(
            "30-callshort", "{sample}_{read_type}.{aligner}-deepvar.{ref}.{chrom}.log"
        )
    container:
        f"{config['container_store']}/{config['deepvariant']}"
    threads: CPU_LOW
    resources:
        mem_mb = lambda wildcards, attempt: 16384 + 8192 * attempt,
        time_hrs = lambda wildcards, attempt: 1 * attempt,
        arch=":arch=skylake"  # docker default built with AVX512
    params:
        tempdir = lambda wildcards: DIR_PROC.joinpath(
            "temp", "deepvariant", wildcards.ref,
            wildcards.sample, wildcards.aligner, wildcards.chrom
        ),
        model = lambda wildcards: config["deepvariant_models"][wildcards.read_type]
    shell:
        "rm -rf {params.tempdir}"
            " && "
        "mkdir -p {params.tempdir}"
            " && "
        "/opt/deepvariant/bin/run_deepvariant --model_type {params.model} "
        "--ref {input.ref} --reads {input.bam} --num_shards {threads} "
        "--output_vcf {output.vcfgz} --regions {wildcards.chrom} "
        "--noruntime_report --novcf_stats_report "
        "--intermediate_results_dir {params.tempdir} &> {log}"
            " ; "
        "rm -rfd {params.tempdir}"


rule run_deepvariant_hifi_calling:
    input:
        vcfs = expand(
            rules.short_call_deepvariant_hifi.output.vcfgz,
            sample=HIFI_SAMPLES,
            read_type=["hifi"],
            aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
            ref=USE_REF_GENOMES,
            chrom=CHROMOSOMES
        )
