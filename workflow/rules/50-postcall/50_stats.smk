
rule compute_callset_summary_stats:
    input:
        vcf=DIR_RES.joinpath("callsets", "{filename}.{variant_type}.vcf.gz"),
        tbi=DIR_RES.joinpath("callsets", "{filename}.{variant_type}.vcf.gz.tbi"),
    output:
        tsv=DIR_RES.joinpath("callsets", "{filename}.{variant_type}.summary-stats.tsv"),
    conda:
        DIR_ENVS.joinpath("pyscript.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
    params:
        script=find_script("vcf_stats_summary"),
        set_vtype=lambda wildcards: (
            " "
            if wildcards.variant_type == "sv"
            else f"--fix-variant-type {wildcards.variant_type} "
        ),
        acc_out=lambda wildcards, output: register_result(output),
    shell:
        "{params.script} --vcf {input.vcf} {params.set_vtype} --output {output.tsv}"


rule compute_callset_vcf_statistics:
    input:
        vcf=DIR_RES.joinpath("callsets", "{filename}.{variant_type}.vcf.gz"),
        tbi=DIR_RES.joinpath("callsets", "{filename}.{variant_type}.vcf.gz.tbi"),
    output:
        txt=DIR_RES.joinpath("callsets", "{filename}.{variant_type}.vcf-stats.txt"),
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
    params:
        acc_out=lambda wildcards, output: register_result(output),
    shell:
        "bcftools stats {input.vcf} > {output.txt}"


rule compute_deeptrio_trio_summary_stats:
    input:
        vcf=DIR_PROC.joinpath(
            "30-callshort",
            "trio-wg",
            "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{vartypes}.vcf.gz",
        ),
        tbi=DIR_PROC.joinpath(
            "30-callshort",
            "trio-wg",
            "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{vartypes}.vcf.gz.tbi",
        ),
    output:
        tsv=DIR_RES.joinpath(
            "callsets",
            "deeptrio-stats",
            "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{vartypes}.summary-stats.tsv",
        ),
    conda:
        DIR_ENVS.joinpath("pyscript.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
    params:
        script=find_script("vcf_stats_summary"),
        set_vtype=lambda wildcards: (
            " "
            if wildcards.vartypes == "sv"
            else f"--fix-variant-type {wildcards.vartypes} "
        ),
        acc_out=lambda wildcards, output: register_result(output),
    shell:
        "{params.script} --vcf {input.vcf} {params.set_vtype} --output {output.tsv}"


rule compute_deepdtrio_duo_summary_stats:
    input:
        vcf=DIR_PROC.joinpath(
            "30-callshort",
            "duo-wg",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{vartypes}.vcf.gz",
        ),
        tbi=DIR_PROC.joinpath(
            "30-callshort",
            "duo-wg",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{vartypes}.vcf.gz.tbi",
        ),
    output:
        tsv=DIR_RES.joinpath(
            "callsets",
            "deepduo-stats",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{vartypes}.summary-stats.tsv",
        ),
    conda:
        DIR_ENVS.joinpath("pyscript.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
    params:
        script=find_script("vcf_stats_summary"),
        set_vtype=lambda wildcards: (
            " "
            if wildcards.vartypes == "sv"
            else f"--fix-variant-type {wildcards.vartypes} "
        ),
        acc_out=lambda wildcards, output: register_result(output),
    shell:
        "{params.script} --vcf {input.vcf} {params.set_vtype} --output {output.tsv}"


rule compute_glnexus_vcf_statistics:
    input:
        vcf=DIR_RES.joinpath(
            "callsets",
            "{joint_type}",
            "{id}_{read_type}.{aligner}-glnexus.{ref}.{vartypes}.trio.vcf.gz",
        ),
        tbi=DIR_RES.joinpath(
            "callsets",
            "{joint_type}",
            "{id}_{read_type}.{aligner}-glnexus.{ref}.{vartypes}.trio.vcf.gz.tbi",
        ),
    output:
        txt=DIR_RES.joinpath(
            "callsets",
            "{joint_type}",
            "{id}_{read_type}.{aligner}-glnexus.{ref}.{vartypes}.vcf-stats.trio.txt",
        ),
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
    params:
        acc_out=lambda wildcards, output: register_result(output),
    shell:
        "bcftools stats {input.vcf} > {output.txt}"
