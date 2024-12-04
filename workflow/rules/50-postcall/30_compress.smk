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


        rule merge_structural_variant_read_tables:
            input:
                tsv = expand(
                    rules.extract_structural_variant_reads.output.tsv,
                    ref=["prg", "prg1", "prg2"],
                    allow_missing=True
                )
            output:
                tsv = DIR_RES.joinpath(
                    "variant_reads", "{sample}_hifi.mm2-sniffles.sv.merged-variant-reads.tsv.gz"
                )
            resources:
                mem_mb=lambda wildcards, attempt: 2048 * attempt
            run:
                import pandas as pd

                concat = []
                for tsv in input.tsv:
                    df = pd.read_csv(tsv, sep="\t", header=0, index_col=0)
                    concat.append(df)

                concat = pd.concat(concat, axis=1, ignore_index=False)
                concat = concat.fillna(0, inplace=False).astype(int)
                concat.sort_index(inplace=True)

                concat.to_csv(output.tsv, sep="\t", header=True, index=True)
            # END OF RUN BLOCK


        rule dump_likely_case_reads:
            input:
                tsv = rules.merge_structural_variant_read_tables.output.tsv
            output:
                lst = DIR_RES.joinpath(
                    "variant_reads", "case_specific",
                    "{sample}_hifi.mm2-sniffles.sv.case-specific-reads.txt"
                )
            run:
                import pandas as pd
                df = pd.read_csv(input.tsv, sep="\t", header=0, index_col=0)

                # 2024-12-04 after discussion w/ Jana and Tobias:
                # relax/extend the selection criteria as follows:
                # opt criterion 1:
                # - read supports a HOM/ALT call in one haploid PRG
                # opt criterion 2:
                # - read supports a HET call in both haploid PRGs
                # criterion 3:
                # - read supports a HOM/ALT call in the diploid PRG (MAPQ 0 scenario)
                # the final selection criterion is then constructed as:
                # select_read = (opt1 OR opt2) AND criterion 3
                # TODO - make column selection generic
                hap_hom = ["PRG1_HOM", "PRG2_HOM"]
                hap_het = ["PRG1_HET", "PRG2_HET"]
                dip_hom = "PRG_HOM"

                select_hap_hom = (df[hap_hom] > 0).any(axis=1)
                select_hap_het = (df[hap_het] > 0).all(axis=1)
                select_dip_hom = (df[dip_hom] > 0)

                selector = (select_hap_hom | select_hap_het) & select_dip

                case_reads = df.loc[selector, :].copy()
                case_read_names = sorted(df.index[selector].values)

                with open(output.lst, "w") as dump:
                    _ = dump.write("\n".join(case_read_names) + "\n")
            # END OF RUN BLOCK


        rule run_all_hifi_variant_reads:
            input:
                tsv = expand(
                    rules.dump_likely_case_reads.output.lst,
                    sample=sorted(set(PAIRED_CASES).intersection(set(HIFI_SAMPLES))),
                )


    rule run_hifi_finalize_sv_callsets_personalized:
        """TODO / the PRG code path can only work w/ SV callers that report the
        variant supporting reads. At the moment, this is only sniffles,
        hence need to adapt the below wildcard lists.
        """
        input:
            vcf = expand(
                DIR_RES.joinpath(
                    "callsets", "{sample}_hifi.{sv_calling_toolchain}.{ref}.sv.vcf.gz"
                ),
                sample=sorted(set(PAIRED_CASES).intersection(set(HIFI_SAMPLES))),
                sv_calling_toolchain=[
                    wildcard for wildcard in HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS
                    if "sniffles" in wildcard
                ],
                ref=["prg", "prg1", "prg2"]
            ),
            txt_stats = expand(
                DIR_RES.joinpath(
                    "callsets", "{sample}_hifi.{sv_calling_toolchain}.{ref}.sv.vcf-stats.txt"
                ),
                sample=sorted(set(PAIRED_CASES).intersection(set(HIFI_SAMPLES))),
                sv_calling_toolchain=[
                    wildcard for wildcard in HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS
                    if "sniffles" in wildcard
                ],
                ref=["prg", "prg1", "prg2"]
            ),
            tsv_stats = expand(
                DIR_RES.joinpath(
                    "callsets", "{sample}_hifi.{sv_calling_toolchain}.{ref}.sv.summary-stats.tsv"
                ),
                sample=sorted(set(PAIRED_CASES).intersection(set(HIFI_SAMPLES))),
                sv_calling_toolchain=[
                    wildcard for wildcard in HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS
                    if "sniffles" in wildcard
                ],
                ref=["prg", "prg1", "prg2"]
            ),
