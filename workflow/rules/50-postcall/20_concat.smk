
localrules: create_chromosome_short_callsets_fofn
rule create_chromosome_short_callsets_fofn:
    input:
        vcfs = expand(
            DIR_PROC.joinpath(
                "50-postcall", "10_split",
                "{{sample}}_{{read_type}}.{{short_calling_toolchain}}.{{ref}}.{{vartypes}}.{chrom}.vcf.gz"),
            chrom=CHROMOSOMES
        ),
        tbis = expand(
            DIR_PROC.joinpath(
                "50-postcall", "10_split",
                "{{sample}}_{{read_type}}.{{short_calling_toolchain}}.{{ref}}.{{vartypes}}.{chrom}.vcf.gz.tbi"),
            chrom=CHROMOSOMES
        )
    output:
        fofn = DIR_PROC.joinpath(
            "50-postcall", "20_concat",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.vcfs.fofn"
        )
    run:
        import pathlib as pl

        # sort files based on the sort order
        # of the chromosomes in the respective
        # yaml pipeline config
        chrom_order = dict((chrom, idx) for idx, chrom in enumerate(CHROMOSOMES))

        unsorted_vcfs = []
        for vcf in input.vcfs:
            this_chrom = vcf.rsplit(".", 3)[-3]
            try:
                this_idx = chrom_order[this_chrom]
            except KeyError:
                err_msg = "50-postcall::20_concat::"
                err_msg += "create_chromosome_short_callsets_fofn: "
                err_msg += f"cannot identify chromosome: {vcf}"
                logerr(err_msg)
                raise
            unsorted_vcfs.append((idx, vcf))
        with open(output.fofn, "w") as fofn:
            for _, vcf in sorted(unsorted_vcfs):
                _ = fofn.write(f"{vcf}\n")
    # END OF RUN BLOCK


rule concat_chromosome_short_callsets:
    input:
        fofn = DIR_PROC.joinpath(
            "50-postcall", "20_concat",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.vcfs.fofn"
        )
    output:
        vcf = DIR_RES.joinpath(
            "callsets",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.vcf.gz"
        ),
        tbi = DIR_RES.joinpath(
            "callsets",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.vcf.gz.tbi"
        )
    log:
        log = DIR_LOG.joinpath(
            "50-postcall", "20_concat",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.bcftools.log"
        )
    wildcard_constraints:
        vartypes = "(" + "|".join(list(config["split_short_calls"].keys())) + ")"
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb = lambda wildcards, attempt: 1024 * attempt
    shell:
        "bcftools concat --file-list {input.fofn} "
        "--compression-level 9 --output {output.vcf} --output-type z &> {log}"
            " && "
        "bcftools tabix -p vcf -f {output.vcf} &>> {log}"


rule run_concat_hifi_short_callsets:
    input:
        vcf = expand(
            DIR_RES.joinpath(
                "callsets",
                "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.vcf.gz"
            ),
            sample=HIFI_SAMPLES,
            read_type=["hifi"],
            short_calling_toolchain=HIFI_SHORT_CALLING_TOOLCHAIN_WILDCARDS,
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys())
        ),
