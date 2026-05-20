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
        vcf=DIR_PROC.joinpath(
            "30-callshort",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{chrom}.vcf.gz",
        ),
    output:
        vcf=DIR_PROC.joinpath(
            "50-postcall",
            "10_split",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.{chrom}.vcf.gz",
        ),
        tbi=DIR_PROC.joinpath(
            "50-postcall",
            "10_split",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.{chrom}.vcf.gz.tbi",
        ),
    log:
        log=DIR_LOG.joinpath(
            "50-postcall",
            "10_split",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.{chrom}.bcftools.log",
        ),
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
    params:
        incl_variants=lambda wildcards: config["split_short_calls"][wildcards.vartypes],
    shell:
        "bcftools view --types {params.incl_variants} --output-type z "
        "--compression-level 9 --output {output.vcf} {input.vcf} &> {log}"
        " && "
        "bcftools tabix -p vcf -f {output.vcf} &>> {log}"


rule split_glnexus_callset:
    input:
        vcf=DIR_PROC.joinpath(
            "15-genotype",
            "{joint_type}",
            "{id}_{read_type}.{aligner}-glnexus.{ref}.{chrom}.vcf.gz",
        ),
    output:
        vcf=DIR_PROC.joinpath(
            "50-postcall",
            "10_split",
            "{joint_type}",
            "{id}_{read_type}.{aligner}-glnexus.{ref}.{vartypes}.{chrom}.vcf.gz",
        ),
        tbi=DIR_PROC.joinpath(
            "50-postcall",
            "10_split",
            "{joint_type}",
            "{id}_{read_type}.{aligner}-glnexus.{ref}.{vartypes}.{chrom}.vcf.gz.tbi",
        ),
    log:
        DIR_LOG.joinpath(
            "50-postcall",
            "10_split",
            "{joint_type}",
            "{id}_{read_type}.{aligner}-glnexus.{ref}.{vartypes}.{chrom}.bcftools.log",
        ),
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
    params:
        incl_variants=lambda wildcards: config["split_short_calls"][wildcards.vartypes],
    shell:
        "bcftools view --types {params.incl_variants} --output-type z "
        "--compression-level 9 --output {output.vcf} {input.vcf} &> {log}"
        " && "
        "bcftools tabix -p vcf -f {output.vcf} &>> {log}"


rule split_deeptrio_trio_callset:
    input:
        vcf=DIR_PROC.joinpath(
            "30-callshort",
            "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{chrom}.vcf.gz",
        ),
    output:
        vcf=DIR_PROC.joinpath(
            "30-callshort",
            "trio-split",
            "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{vartypes}.{chrom}.vcf.gz",
        ),
        tbi=DIR_PROC.joinpath(
            "30-callshort",
            "trio-split",
            "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{vartypes}.{chrom}.vcf.gz.tbi",
        ),
    log:
        DIR_LOG.joinpath(
            "30-callshort",
            "trio-split",
            "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{vartypes}.{chrom}.bcftools.log",
        ),
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
    params:
        incl_variants=lambda wildcards: config["split_short_calls"][wildcards.vartypes],
    shell:
        "bcftools view --types {params.incl_variants} --output-type z "
        "--compression-level 9 --output {output.vcf} {input.vcf} &> {log}"
        " && "
        "bcftools tabix -p vcf -f {output.vcf} &>> {log}"


rule split_deeptrio_duo_callset:
    input:
        vcf=DIR_PROC.joinpath(
            "30-callshort",
            "duo",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{chrom}.vcf.gz",
        ),
    output:
        vcf=DIR_PROC.joinpath(
            "30-callshort",
            "duo-split",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{vartypes}.{chrom}.vcf.gz",
        ),
        tbi=DIR_PROC.joinpath(
            "30-callshort",
            "duo-split",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{vartypes}.{chrom}.vcf.gz.tbi",
        ),
    log:
        DIR_LOG.joinpath(
            "30-callshort",
            "duo-split",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{vartypes}.{chrom}.bcftools.log",
        ),
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
    params:
        incl_variants=lambda wildcards: config["split_short_calls"][wildcards.vartypes],
    shell:
        "bcftools view --types {params.incl_variants} --output-type z "
        "--compression-level 9 --output {output.vcf} {input.vcf} &> {log}"
        " && "
        "bcftools tabix -p vcf -f {output.vcf} &>> {log}"
