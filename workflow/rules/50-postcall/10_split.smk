"""
Module to split the likely large
callsets for short variants into
SNV
InDel
Other
before concatenating the per-chromosome
callsets into a whole-genome callset.
"""

rule split_short_callset:
    input:
        vcf = DIR_PROC.joinpath(
            "30-callshort", "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{chrom}.vcf.gz"
        )
    output:
        vcf = DIR_PROC.joinpath(
            "50-postcall", "10_split",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.{chrom}.vcf.gz"
        ),
        tbi = DIR_PROC.joinpath(
            "50-postcall", "10_split",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.{chrom}.vcf.gz.tbi"
        )
    log:
        log = DIR_LOG.joinpath(
            "50-postcall", "10_split",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.{chrom}.bcftools.log"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb = lambda wildcards, attempt: 1024 * attempt
    params:
        incl_variants = lambda wildcards: config["split_short_calls"][wildcards.vartypes]
    shell:
        "bcftools view --types {params.incl_variants} --output-type z "
        "--compression-level 9 --output {output.vcf} {input.vcf} &> {log}"
            " && "
        "bcftools tabix -p vcf -f {output.vcf} &>> {log}"
