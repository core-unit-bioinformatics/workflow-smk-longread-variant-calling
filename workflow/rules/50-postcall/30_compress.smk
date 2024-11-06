"""
Module to pull all SV callsets
into the results folder. Currently,
all callers produce an uncompressed
VCF output.
If postprocessing becomes necessary
for the SV callsets, it should be
implemented here.
"""

rule compress_index_sv_callset:
    input:
        vcf = DIR_PROC.joinpath(
            "40-callsv", "{filename}.vcf"
        )
    output:
        vcf = DIR_RES.joinpath(
            "callsets", "{filename}.sv.vcf.gz"
        ),
        tbi = DIR_RES.joinpath(
            "callsets", "{filename}.sv.vcf.gz.tbi"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    params:
        acc_out = lambda wildcards, output: register_result(output)
    shell:
        "bgzip --keep --stdout --compress-level 9 "
        "{input.vcf} > {output.vcf}"
            " && "
        "bcftools tabix -p vcf -f {output.vcf}"


rule run_hifi_finalize_sv_callsets:
    input:
        vcf = expand(
            DIR_RES.joinpath(
                "callsets", "{sample}_hifi.{sv_calling_toolchain}.{ref}.sv.vcf.gz"
            ),
            sample=HIFI_SAMPLES,
            sv_calling_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
            ref=USE_REF_GENOMES
        ),
        txt_stats = expand(
            DIR_RES.joinpath(
                "callsets", "{sample}_hifi.{sv_calling_toolchain}.{ref}.sv.vcf-stats.txt"
            ),
            sample=HIFI_SAMPLES,
            sv_calling_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
            ref=USE_REF_GENOMES
        ),
        tsv_stats = expand(
            DIR_RES.joinpath(
                "callsets", "{sample}_hifi.{sv_calling_toolchain}.{ref}.sv.summary-stats.tsv"
            ),
            sample=HIFI_SAMPLES,
            sv_calling_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
            ref=USE_REF_GENOMES
        ),


if SAMPLE_PAIRS is not None:

    if any("sniffles" in toolchain_wildcard for toolchain_wildcard in RUN_HIFI_SV_CALLING_TOOLCHAIN):

        # this can only work for callers that can report reads --- not pbsv, for example

        rule extract_structural_variant_reads:
            input:
                vcf = DIR_RES.joinpath(
                    "callsets", "{sample}_hifi.mm2-sniffles.{ref}.sv.vcf.gz"
                ),
                subtract = lambda wildcards: DIR_RES.joinpath(
                        "callsets", f"{SAMPLE_PAIRS[wildcards.sample]}_hifi.mm2-sniffles.prg.sv.vcf.gz"
                    )
            output:
                tsv = DIR_RES.joinpath(
                    "variant_reads", "{sample}_hifi.mm2-sniffles.{ref}.sv.variant-reads.tsv.gz"
                )
            log:
                DIR_LOG.joinpath(
                    "variant_reads", "{sample}_hifi.mm2-sniffles.{ref}.sv.variant-reads.log"
                )
            wildcard_constraints:
                ref = "(prg|prg1|prg2)"
            conda:
                DIR_ENVS.joinpath("pyscript.yaml")
            resources:
                mem_mb=lambda wildcards, attempt: 1024 * attempt
            params:
                script=find_script("extract_variant_reads")
            shell:
                "zcat {input.vcf}"
                    " | "
                "{params.script} --subtract-sites {input.subtract} --reference-id {wildcards.ref} "
                "--variant-reads {output.tsv} > {log}"


        rule run_all_hifi_variant_reads:
            input:
                tsv = expand(
                    rules.extract_structural_variant_reads.output.tsv,
                    sample=CASE_SAMPLES,
                    ref=["prg", "prg1", "prg2"]
                )


    rule run_hifi_finalize_sv_callsets_personalized:
        input:
            vcf = expand(
                DIR_RES.joinpath(
                    "callsets", "{sample}_hifi.{sv_calling_toolchain}.{ref}.sv.vcf.gz"
                ),
                sample=HIFI_SAMPLES,
                sv_calling_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
                ref=["prg", "prg1", "prg2"]
            ),
            txt_stats = expand(
                DIR_RES.joinpath(
                    "callsets", "{sample}_hifi.{sv_calling_toolchain}.{ref}.sv.vcf-stats.txt"
                ),
                sample=HIFI_SAMPLES,
                sv_calling_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
                ref=["prg", "prg1", "prg2"]
            ),
            tsv_stats = expand(
                DIR_RES.joinpath(
                    "callsets", "{sample}_hifi.{sv_calling_toolchain}.{ref}.sv.summary-stats.tsv"
                ),
                sample=HIFI_SAMPLES,
                sv_calling_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
                ref=["prg", "prg1", "prg2"]
            ),
