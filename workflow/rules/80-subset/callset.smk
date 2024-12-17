
"""
This module will only be executed if a code path
leading to a personalized reference genome was
executed. That is, the output of the rule
rules::50-postcall::30_compress::dump_likely_case_reads
exists
"""

if SAMPLE_PAIRS is not None:

    rule create_subset_callset_by_case_reads:
        input:
            read_ids = rules.dump_likely_case_reads.output.lst,
            callset = DIR_PROC.joinpath(
                "40-callsv", "{sample}_hifi.mm2-sniffles.{ref}.vcf"
            )
        output:
            vcf = DIR_PROC.joinpath(
                "80-subset", "case_specific_callset",
                "{sample}_hifi.mm2-sniffles.{ref}.case-read-filt.vcf"
            ),
            summary = DIR_PROC.joinpath(
                "80-subset", "case_specific_callset",
                "{sample}_hifi.mm2-sniffles.{ref}.case-read-filt.summary.txt"
            )
        resources:
            mem_mb=lambda wildcards, attempt: 4096 * attempt,
            time_hrs=lambda wildcards, attempt: attempt * attempt
        run:
            import io
            case_reads = set(open(input.read_ids).read().strip().split())

            VCF_INFO_COLUMN_INDEX = 7

            out_buffer = io.StringIO()
            total_calls = 0
            selected_calls = 0
            selected_support = []
            skipped_support = []
            with open(input.callset, "r") as vcf:
                for line in vcf:
                    if line.startswith("#"):
                        out_buffer.write(line)
                        continue
                    info_column = line.split()[VCF_INFO_COLUMN_INDEX]
                    # that is likely sniffles specific --- how to make this config param?
                    assert "RNAMES" in line
                    for entry in info_column.split(";"):
                        if not entry.startswith("RNAMES"):
                            continue
                        total_calls += 1
                        read_names = set(entry.strip("RNAMES=").split(","))
                        total_reads = len(read_names)
                        support_reads = len(read_names.intersection(case_reads))
                        if support_reads > 0:
                            pct_case_support = round(support_reads/total_reads * 100, 2)
                            if pct_case_support > 50:
                                selected_calls += 1
                                out_buffer.write(line)
                                selected_support.append(pct_case_support)
                            else:
                                skipped_support.append(pct_case_support)
                        break

            with open(output.vcf, "w") as dump:
                _ = dump.write(out_buffer.getvalue())

            selected_pct = round(selected_calls/total_calls * 100, 4)
            with open(output.summary, "w") as dump:
                _ = dump.write(f"total_calls\t{total_calls}\n")
                _ = dump.write(f"selected_calls\t{selected_calls}\n")
                _ = dump.write(f"selected_pct\t{selected_pct}\n")
                for value in selected_support:
                    _ = dump.write(f"selected_support\t{value}\n")
                for value in skipped_support:
                    _ = dump.write(f"skipped_support\t{value}\n")
        # END OF RUN BLOCK


    rule compress_index_sv_case_read_callset:
        """TODO
        this is a quasi-duplicate of the same
        rule in rules::50-postcall::30_compress.smk
        """
        input:
            vcf = rules.create_subset_callset_by_case_reads.output.vcf
        output:
            vcf = DIR_RES.joinpath(
                "callsets", "prg_derived", "{sample}_hifi.mm2-sniffles.{ref}.sv.case-read-filt.vcf.gz"
            ),
            tbi = DIR_RES.joinpath(
                "callsets", "prg_derived", "{sample}_hifi.mm2-sniffles.{ref}.sv.case-read-filt.vcf.gz.tbi"
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


    rule run_all_subset_callsets_case_reads:
        input:
            vcf = expand(
                rules.compress_index_sv_case_read_callset.output.vcf,
                sample=PAIRED_CASES,
                ref=USE_REF_GENOMES
            )


if CASE_GROUPS:

    rule split_multisample_sv_into_case_callsets:
        input:
            multisample_vcf = DIR_PROC.joinpath("40-callsv", "SAMPLES_{read_type}.{sv_calling_toolchain}.{ref}.vcf"),
        output:
            vcf = DIR_RES.joinpath(
                "callsets", "subsets", "SAMPLES_{read_type}.{sv_calling_toolchain}.{ref}.sv.case-{case_group}.vcf.gz"
            ),
            tbi = DIR_RES.joinpath(
                "callsets", "subsets", "SAMPLES_{read_type}.{sv_calling_toolchain}.{ref}.sv.case-{case_group}.vcf.gz.tbi"
            ),
            sample_matrix = DIR_RES.joinpath(
                "callsets", "subsets", "SAMPLES_{read_type}.{sv_calling_toolchain}.{ref}.sv.case-{case_group}.sample-matrix.tsv.gz"
            ),
            stats = DIR_RES.joinpath(
                "callsets", "subsets", "SAMPLES_{read_type}.{sv_calling_toolchain}.{ref}.sv.case-{case_group}.support-stats.tsv.gz"
            )
        conda:
            DIR_ENVS.joinpath("pyscript.yaml")
        params:
            script=find_script("collect_shared_stats"),
            baseline_samples = lambda wildcards: get_sample_group_list("baseline"),
            case_samples = lambda wildcards: get_sample_group_list(wildcards.case_group),
            caller=lambda wildcards: wildcards.sv_calling_toolchain.split("-")[-1].strip()
        resources:
            mem_mb=lambda wildcards, attempt: 4096 * attempt,
            time_hrs=lambda wildcards, attempt: attempt
        shell:
            "cat {input.multisample_vcf}"
                " | "
            "{params.script} --calling-algorithm {params.caller} "
            "{params.baseline_samples} {params.case_samples} "
            "--sample-matrix {output.sample_matrix} --stats-out {output.stats} "
            "--vcf-subset"
                " | "
            "bgzip --keep --stdout --compress-level 9 > {output.vcf}"
                " && "
            "bcftools tabix -p vcf -f {output.vcf}"


    rule split_multisample_sv_case_callsets_by_sample:
        """TODO
        this rule is somewhat of a workaround b/c the VCF annotation
        pipeline can currently only process single-sample VCFs.
        """
        input:
            vcf = rules.split_multisample_sv_into_case_callsets.output.vcf,
            tbi = rules.split_multisample_sv_into_case_callsets.output.tbi
        output:
            vcf = DIR_RES.joinpath(
                "callsets", "subsets", "by_sample",
                "{sample}_{read_type}.{sv_calling_toolchain}.{ref}.sv.case-{case_group}.vcf.gz"
            ),
            tbi = DIR_RES.joinpath(
                "callsets", "subsets", "by_sample",
                "{sample}_{read_type}.{sv_calling_toolchain}.{ref}.sv.case-{case_group}.vcf.gz.tbi"
            ),
        conda:
            DIR_ENVS.joinpath("biotools.yaml")
        resources:
            mem_mb=lambda wildcards, attempt: 2048 * attempt
        shell:
            "bcftools view -s {wildcards.sample} --output-type z9 --output {output.vcf}"
                " && "
            "tabix -p vcf -f {output.vcf}"


    rule run_all_subset_sv_case_callsets:
        input:
            vcf = expand(
                rules.split_multisample_sv_into_case_callsets.output.vcf,
                read_type=["hifi"],
                sv_calling_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
                ref=USE_REF_GENOMES,
                case_group=sorted(CASE_GROUPS.keys())
            ),
            split_vcf = expand(
                rules.split_multisample_sv_case_callsets_by_sample.output.vcf,
                sample=sorted(CASE_GROUPS["all"]),
                read_type=["hifi"],
                sv_calling_toolchain=HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS,
                ref=USE_REF_GENOMES,
                case_group="all"
            ),
