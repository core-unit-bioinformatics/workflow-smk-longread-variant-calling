
localrules:
    create_chromosome_short_callsets_fofn,


rule create_chromosome_short_callsets_fofn:
    input:
        vcfs=expand(
            DIR_PROC.joinpath(
                "50-postcall",
                "10_split",
                "{{sample}}_{{read_type}}.{{short_calling_toolchain}}.{{ref}}.{{vartypes}}.{chrom}.vcf.gz",
            ),
            chrom=CHROMOSOMES,
        ),
        tbis=expand(
            DIR_PROC.joinpath(
                "50-postcall",
                "10_split",
                "{{sample}}_{{read_type}}.{{short_calling_toolchain}}.{{ref}}.{{vartypes}}.{chrom}.vcf.gz.tbi",
            ),
            chrom=CHROMOSOMES,
        ),
    output:
        fofn=DIR_PROC.joinpath(
            "50-postcall",
            "20_concat",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.vcfs.fofn",
        ),
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
            unsorted_vcfs.append((this_idx, vcf))
        with open(output.fofn, "w") as fofn:
            for _, vcf in sorted(unsorted_vcfs):
                _ = fofn.write(f"{vcf}\n")


# END OF RUN BLOCK
rule concat_chromosome_short_callsets:
    input:
        fofn=DIR_PROC.joinpath(
            "50-postcall",
            "20_concat",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.vcfs.fofn",
        ),
    output:
        vcf=DIR_RES.joinpath(
            "callsets",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.vcf.gz",
        ),
        tbi=DIR_RES.joinpath(
            "callsets",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.vcf.gz.tbi",
        ),
    log:
        log=DIR_LOG.joinpath(
            "50-postcall",
            "20_concat",
            "{sample}_{read_type}.{short_calling_toolchain}.{ref}.{vartypes}.bcftools.log",
        ),
    wildcard_constraints:
        vartypes="(" + "|".join(list(config["split_short_calls"].keys())) + ")",
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
    params:
        acc_out=lambda wildcards, output: register_result(output),
    shell:
        "bcftools concat --file-list {input.fofn} "
        "--output {output.vcf} --output-type z &> {log}"
        " && "
        "bcftools tabix -p vcf -f {output.vcf} &>> {log}"

localrules:
    create_deeptrio_trio_fofn,


rule create_chromosome_glnexus_fofn:
    input:
        vcfs=expand(
            DIR_PROC.joinpath(
                "50-postcall",
                "10_split",
                "{{joint_type}}",
                "{{id}}_{{read_type}}.{{aligner}}-glnexus.{{ref}}.{{vartypes}}.{chrom}.vcf.gz",
            ),
            chrom=CHROMOSOMES,
        ),
        tbis=expand(
            DIR_PROC.joinpath(
                "50-postcall",
                "10_split",
                "{{joint_type}}",
                "{{id}}_{{read_type}}.{{aligner}}-glnexus.{{ref}}.{{vartypes}}.{chrom}.vcf.gz.tbi",
            ),
            chrom=CHROMOSOMES,
        ),
    output:
        fofn=DIR_PROC.joinpath(
            "50-postcall",
            "20_concat",
            "{joint_type}",
            "{id}_{read_type}.{aligner}-glnexus.{ref}.{vartypes}.trio.vcfs.fofn",
        ),
    run:
        import pathlib as pl

        chrom_order = dict((chrom, idx) for idx, chrom in enumerate(CHROMOSOMES))
        unsorted_vcfs = []
        for vcf in input.vcfs:
            this_chrom = vcf.rsplit(".", 3)[-3]
            try:
                this_idx = chrom_order[this_chrom]
            except KeyError:
                err_msg = "50-postcall::20_concat::"
                err_msg += "create_chromosome_glnexus_fofn: "
                err_msg += f"cannot identify chromosome: {vcf}"
                logerr(err_msg)
                raise
            unsorted_vcfs.append((this_idx, vcf))
        with open(output.fofn, "w") as fofn:
            for _, vcf in sorted(unsorted_vcfs):
                _ = fofn.write(f"{vcf}\n")


# END OF RUN BLOCK
rule concat_chromosome_glnexus_callsets:
    input:
        fofn=DIR_PROC.joinpath(
            "50-postcall",
            "20_concat",
            "{joint_type}",
            "{id}_{read_type}.{aligner}-glnexus.{ref}.{vartypes}.trio.vcfs.fofn",
        ),
    output:
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
    log:
        DIR_LOG.joinpath(
            "50-postcall",
            "20_concat",
            "{joint_type}",
            "{id}_{read_type}.{aligner}-glnexus.{ref}.{vartypes}.trio.concat.log",
        ),
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    shell:
        "bcftools concat --file-list {input.fofn} "
        "--output {output.vcf} --output-type z &> {log}"
        " && "
        "bcftools tabix -p vcf -f {output.vcf} &>> {log}"


localrules:
    create_deeptrio_trio_fofn,


rule create_chromosome_deeptrio_trio_fofn:
    input:
        vcfs=expand(
            DIR_PROC.joinpath(
                "30-callshort",
                "trio-split",
                "{{sample}}_{{read_type}}.{{aligner}}-deeptrio.{{role}}.{{ref}}.{{vartypes}}.{chrom}.vcf.gz",
            ),
            chrom=CHROMOSOMES,
        ),
        tbi=expand(
            DIR_PROC.joinpath(
                "30-callshort",
                "trio-split",
                "{{sample}}_{{read_type}}.{{aligner}}-deeptrio.{{role}}.{{ref}}.{{vartypes}}.{chrom}.vcf.gz",
            ),
            chrom=CHROMOSOMES,
        ),
    output:
        fofn=DIR_PROC.joinpath(
            "30-callshort",
            "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{vartypes}.vcfs.fofn",
        ),
    run:
        import pathlib as pl

        chrom_order = dict((chrom, idx) for idx, chrom in enumerate(CHROMOSOMES))
        unsorted_vcfs = []
        for vcf in input.vcfs:
            this_chrom = vcf.rsplit(".", 3)[-3]
            try:
                this_idx = chrom_order[this_chrom]
            except KeyError:
                err_msg = "50-postcall::20_concat::"
                err_msg += "create_chromosome_glnexus_fofn: "
                err_msg += f"cannot identify chromosome: {vcf}"
                logerr(err_msg)
                raise
            unsorted_vcfs.append((this_idx, vcf))
        with open(output.fofn, "w") as fofn:
            for _, vcf in sorted(unsorted_vcfs):
                _ = fofn.write(f"{vcf}\n")


# END OF RUN BLOCK
rule concat_chromosome_deeptrio_trio_callsets:
    input:
        fofn=DIR_PROC.joinpath(
            "30-callshort",
            "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{vartypes}.vcfs.fofn",
        ),
    output:
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
    log:
        DIR_LOG.joinpath(
            "30-callshort",
            "trio-wg",
            "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{vartypes}.bcftools.log",
        ),
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    shell:
        "bcftools concat --file-list {input.fofn} "
        "--output {output.vcf} --output-type z &> {log}"
        " && "
        "bcftools tabix -p vcf -f {output.vcf} &>> {log}"


localrules:
    create_deeptrio_duo_fofn,


rule create_chromosome_deeptrio_duo_fofn:
    input:
        vcfs=expand(
            DIR_PROC.joinpath(
                "30-callshort",
                "duo-split",
                "{{sample}}_{{read_type}}.{{aligner}}-deeptrio.duo.{{role}}.{{ref}}.{{vartypes}}.{chrom}.vcf.gz",
            ),
            chrom=CHROMOSOMES,
        ),
        tbi=expand(
            DIR_PROC.joinpath(
                "30-callshort",
                "duo-split",
                "{{sample}}_{{read_type}}.{{aligner}}-deeptrio.duo.{{role}}.{{ref}}.{{vartypes}}.{chrom}.vcf.gz",
            ),
            chrom=CHROMOSOMES,
        ),
    output:
        fofn=DIR_PROC.joinpath(
            "30-callshort",
            "duo",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{vartypes}.vcfs.fofn",
        ),
    run:
        import pathlib as pl

        chrom_order = dict((chrom, idx) for idx, chrom in enumerate(CHROMOSOMES))
        unsorted_vcfs = []
        for vcf in input.vcfs:
            this_chrom = vcf.rsplit(".", 3)[-3]
            try:
                this_idx = chrom_order[this_chrom]
            except KeyError:
                err_msg = "50-postcall::20_concat::"
                err_msg += "create_chromosome_glnexus_fofn: "
                err_msg += f"cannot identify chromosome: {vcf}"
                logerr(err_msg)
                raise
            unsorted_vcfs.append((this_idx, vcf))
        with open(output.fofn, "w") as fofn:
            for _, vcf in sorted(unsorted_vcfs):
                _ = fofn.write(f"{vcf}\n")


# END OF RUN BLOCK
rule concat_chromosome_deeptrio_duo_callsets:
    input:
        fofn=DIR_PROC.joinpath(
            "30-callshort",
            "duo",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{vartypes}.vcfs.fofn",
        ),
    output:
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
    log:
        DIR_LOG.joinpath(
            "30-callshort",
            "duo-wg",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{vartypes}.bcftools.log",
        ),
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    shell:
        "bcftools concat --file-list {input.fofn} "
        "--output {output.vcf} --output-type z &> {log}"
        " && "
        "bcftools tabix -p vcf -f {output.vcf} &>> {log}"


rule run_concat_hifi_short_callsets:
    input:
        vcf=expand(
            DIR_RES.joinpath(
                "callsets",
                "{sample}_hifi.{short_calling_toolchain}.{ref}.{vartypes}.vcf.gz",
            ),
            sample=HIFI_SAMPLES,
            short_calling_toolchain=HIFI_SHORT_CALLING_TOOLCHAIN_WILDCARDS,
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),
        txt_stats=expand(
            DIR_RES.joinpath(
                "callsets",
                "{sample}_hifi.{sv_calling_toolchain}.{ref}.{vartypes}.vcf-stats.txt",
            ),
            sample=HIFI_SAMPLES,
            sv_calling_toolchain=HIFI_SHORT_CALLING_TOOLCHAIN_WILDCARDS,
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),
        tsv_stats=expand(
            DIR_RES.joinpath(
                "callsets",
                "{sample}_hifi.{sv_calling_toolchain}.{ref}.{vartypes}.summary-stats.tsv",
            ),
            sample=HIFI_SAMPLES,
            sv_calling_toolchain=HIFI_SHORT_CALLING_TOOLCHAIN_WILDCARDS,
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),


rule run_concat_hifi_trio_callsets:
    input:
        # --- TRIO WHOLE-GENOME VCFs ---
        trio_joint_vcfs=expand(
            DIR_RES.joinpath(
                "callsets",
                "{joint_type}",
                "{id}_hifi.{aligner}-glnexus.{ref}.{vartypes}.trio.vcf.gz",
            ),
            joint_type=["trio_joint"],
            id=SINGLE_CHILD_FAMILIES,
            aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),
        # --- DUO WHOLE-GENOME VCFs ---
        duo_joint_vcfs=expand(
            DIR_RES.joinpath(
                "callsets",
                "{joint_type}",
                "{id}_hifi.{aligner}-glnexus.{ref}.{vartypes}.trio.vcf.gz",
            ),
            joint_type=["duo_joint"],
            id=DUO_CHILDREN,
            aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),
        # --- FAMILY WHOLE-GENOME VCFs ---
        family_joint_vcfs=expand(
            DIR_RES.joinpath(
                "callsets",
                "{joint_type}",
                "{id}_hifi.{aligner}-glnexus.{ref}.{vartypes}.trio.vcf.gz",
            ),
            joint_type=["family_joint"],
            id=MULTI_CHILD_FAMILIES,
            aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),
        # --- TRIO Joint STATS ---
        trio_joint_txt_stats=expand(
            DIR_RES.joinpath(
                "callsets",
                "{joint_type}",
                "{id}_hifi.{aligner}-glnexus.{ref}.{vartypes}.vcf-stats.trio.txt",
            ),
            joint_type=["trio_joint"],
            id=SINGLE_CHILD_FAMILIES,
            aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),
        # --- DUO Joint STATS ---
        duo_joint_txt_stats=expand(
            DIR_RES.joinpath(
                "callsets",
                "{joint_type}",
                "{id}_hifi.{aligner}-glnexus.{ref}.{vartypes}.vcf-stats.trio.txt",
            ),
            joint_type=["duo_joint"],
            id=DUO_CHILDREN,
            aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),
        # --- FAMILY Joint STATS ---
        family_joint_txt_stats=expand(
            DIR_RES.joinpath(
                "callsets",
                "{joint_type}",
                "{id}_hifi.{aligner}-glnexus.{ref}.{vartypes}.vcf-stats.trio.txt",
            ),
            joint_type=["family_joint"],
            id=MULTI_CHILD_FAMILIES,
            aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),
        trio_single_tsv_stats=expand(
            DIR_RES.joinpath(
                "callsets",
                "deeptrio-stats",
                "{sample}_{read_type}.{aligner}-deeptrio.{role}.{ref}.{vartypes}.summary-stats.tsv",
            ),
            role=["child", "mother", "father"],
            sample=TRIO_CHILDREN,
            read_type=["hifi"],
            aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),
        duo_single_tsv_stats=expand(
            DIR_RES.joinpath(
                "callsets",
                "deepduo-stats",
                "{sample}_{read_type}.{aligner}-deeptrio.duo.{role}.{ref}.{vartypes}.summary-stats.tsv",
            ),
            role=["child", "parent"],
            sample=DUO_CHILDREN,
            read_type=["hifi"],
            aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
            ref=USE_REF_GENOMES,
            vartypes=list(config["split_short_calls"].keys()),
        ),
