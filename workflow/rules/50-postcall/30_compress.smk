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
    shell:
        "bgzip --keep --stdout --compress-level 9 "
        "> {output.vcf}"
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
        )
