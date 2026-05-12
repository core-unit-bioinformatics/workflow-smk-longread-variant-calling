
localrules:
    create_sample_genotypes_fofn,


rule create_sample_genotypes_fofn:
    input:
        vcfs=expand(
            DIR_PROC.joinpath(
                "15-genotype",
                "genotyped_samples",
                "{sample}_{read_type}_{ref}_{panel}.pgt.{allele_repr}.vcf.gz",
            ),
            sample=PAIRED_SAMPLES,
            allow_missing=True,
        ),
        tbi=expand(
            DIR_PROC.joinpath(
                "15-genotype",
                "genotyped_samples",
                "{sample}_{read_type}_{ref}_{panel}.pgt.{allele_repr}.vcf.gz.tbi",
            ),
            sample=PAIRED_SAMPLES,
            allow_missing=True,
        ),
    output:
        fofn=DIR_PROC.joinpath(
            "15-genotype",
            "merge_genotyped",
            "file_listings",
            "SAMPLES_{read_type}_{ref}_{panel}.pgt.{allele_repr}.fofn",
        ),
    wildcard_constraints:
        allele_repr="(malc|balc)",
    run:
        import pathlib as pl
        import io

        buffer = io.StringIO()
        for rel_path in sorted(input.vcfs):
            full_path = WORKDIR.joinpath(rel_path)
            assert full_path.is_file()
            buffer.write(rel_path + "\n")
        with open(output.fofn, "w") as dump:
            _ = dump.write(buffer.getvalue())


# END OF RUN BLOCK
rule region_merge_and_fill_sample_genotypes:
    input:
        fofn=rules.create_sample_genotypes_fofn.output.fofn,
    output:
        vcf=DIR_PROC.joinpath(
            "15-genotype",
            "merge_genotyped",
            "by_chrom",
            "SAMPLES_{read_type}_{ref}_{panel}.pgt.{allele_repr}.{chrom}.vcf.gz",
        ),
        tbi=DIR_PROC.joinpath(
            "15-genotype",
            "merge_genotyped",
            "by_chrom",
            "SAMPLES_{read_type}_{ref}_{panel}.pgt.{allele_repr}.{chrom}.vcf.gz.tbi",
        ),
    log:
        DIR_LOG.joinpath(
            "15-genotype",
            "merge_genotyped",
            "by_chrom",
            "SAMPLES_{read_type}_{ref}_{panel}.pgt.{allele_repr}.{chrom}.bcftools.log",
        ),
    benchmark:
        DIR_RSRC.joinpath(
            "15-genotype",
            "merge_genotyped",
            "by_chrom",
            "SAMPLES_{read_type}_{ref}_{panel}.pgt.{allele_repr}.{chrom}.bcftools.rsrc",
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_MEDIUM
    resources:
        mem_mb=lambda wildcards, attempt: 32768 + 32768 * attempt,
        time_hrs=lambda wildcards, attempt: attempt,
    shell:
        "bcftools merge --regions {wildcards.chrom} --file-list {input.fofn} --threads {threads}"
        " | "
        "bcftools plugin fill-tags -O z9 -o {output.vcf} /dev/stdin -- -t AN,AC,AF &> {log}"
        " && "
        "tabix -p vcf --threads {threads} {output.vcf}"


localrules:
    create_merged_chrom_genotypes_fofn,


rule create_merged_chrom_genotypes_fofn:
    input:
        vcfs=expand(
            rules.region_merge_and_fill_sample_genotypes.output.vcf,
            chrom=CHROMOSOMES,
            allow_missing=True,
        ),
        tbi=expand(
            rules.region_merge_and_fill_sample_genotypes.output.tbi,
            chrom=CHROMOSOMES,
            allow_missing=True,
        ),
    output:
        fofn=DIR_PROC.joinpath(
            "15-genotype",
            "merge_genotyped",
            "file_listings",
            "SAMPLES_{read_type}_{ref}_{panel}.pgt.{allele_repr}.chrom.fofn",
        ),
    wildcard_constraints:
        allele_repr="(malc|balc)",
    run:
        import pathlib as pl
        import io

        buffer = io.StringIO()
        for rel_path in sorted(input.vcfs):
            full_path = WORKDIR.joinpath(rel_path)
            assert full_path.is_file()
            buffer.write(rel_path + "\n")
        with open(output.fofn, "w") as dump:
            _ = dump.write(buffer.getvalue())


# END OF RUN BLOCK
rule concat_region_sample_genotypes:
    input:
        fofn=rules.create_merged_chrom_genotypes_fofn.output.fofn,
    output:
        vcf=DIR_RES.joinpath(
            "genotyping",
            "multi_sample",
            "SAMPLES_{read_type}_{ref}_{panel}.pgt.{allele_repr}.wg.vcf.gz",
        ),
        tbi=DIR_RES.joinpath(
            "genotyping",
            "multi_sample",
            "SAMPLES_{read_type}_{ref}_{panel}.pgt.{allele_repr}.wg.vcf.gz.tbi",
        ),
    benchmark:
        DIR_RSRC.joinpath(
            "genotyping",
            "multi_sample",
            "SAMPLES_{read_type}_{ref}_{panel}.pgt.{allele_repr}.wg.bcftools.rsrc",
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb=lambda wildcards, attempt: 24576 + 24576 * attempt,
        time_hrs=lambda wildcards, attempt: 11 * attempt,
    shell:
        "bcftools concat --threads {threads} --output-type z9 --output {output.vcf} --file-list {input.fofn}"
        " && "
        "tabix -p vcf --threads {threads} {output.vcf}"


rule glnexus_trio_joint:
    input:
        gvcf_child=DIR_PROC.joinpath(
            "30-callshort",
            "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.child.{ref}.{chrom}.g.vcf.gz",
        ),
        gvcf_mother=DIR_PROC.joinpath(
            "30-callshort",
            "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.mother.{ref}.{chrom}.g.vcf.gz",
        ),
        gvcf_father=DIR_PROC.joinpath(
            "30-callshort",
            "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.father.{ref}.{chrom}.g.vcf.gz",
        ),
    output:
        vcfgz=DIR_PROC.joinpath(
            "15-genotype",
            "trio_joint",
            "{sample}_{read_type}.{aligner}-glnexus.{ref}.{chrom}.vcf.gz",
        ),
    log:
        DIR_LOG.joinpath(
            "15-genotype",
            "trio_joint",
            "{sample}_{read_type}.{aligner}-glnexus.{ref}.{chrom}.log",
        ),
    container:
        f"{CONTAINER_STORE}/{config['glnexus']}"
    threads: CPU_LOW
    params:
        glnexus_preset=config["glnexus_preset"],
    shell:
        "glnexus_cli --config {params.glnexus_preset} "
        "--threads {threads} "
        "{input.gvcf_child} {input.gvcf_mother} {input.gvcf_father} "
        " | bcftools view -Oz -o {output.vcfgz} "
        " &> {log}"


rule glnexus_duo_joint:
    input:
        gvcf_child=DIR_PROC.joinpath(
            "30-callshort",
            "duo",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.child.{ref}.{chrom}.g.vcf.gz",
        ),
        gvcf_parent=DIR_PROC.joinpath(
            "30-callshort",
            "duo",
            "{sample}_{read_type}.{aligner}-deeptrio.duo.parent.{ref}.{chrom}.g.vcf.gz",
        ),
    output:
        vcfgz=DIR_PROC.joinpath(
            "15-genotype",
            "duo_joint",
            "{sample}_{read_type}.{aligner}-glnexus.{ref}.{chrom}.vcf.gz",
        ),
    log:
        DIR_LOG.joinpath(
            "15-genotype",
            "duo_joint",
            "{sample}_{read_type}.{aligner}-glnexus.{ref}.{chrom}.log",
        ),
    container:
        f"{CONTAINER_STORE}/{config['glnexus']}"
    threads: CPU_LOW
    params:
        glnexus_preset=config["glnexus_preset"],
    shell:
        "glnexus_cli --config {params.glnexus_preset} "
        "--threads {threads} "
        "{input.gvcf_child} {input.gvcf_parent} "
        " | bcftools view -Oz -o {output.vcfgz} "
        " &> {log}"


rule glnexus_family_joint:
    input:
        gvcfs_children=lambda wildcards: expand(
            rules.short_call_deeptrio.output.gvcf_child,
            sample=FAMILY_CHILDREN[wildcards.family],
            allow_missing=True,
        ),
        gvcf_mother=lambda wildcards: expand(
            rules.short_call_deeptrio.output.gvcf_mother,
            sample=FAMILY_REPRESENTATIVE_CHILD[wildcards.family],
            allow_missing=True,
        ),
        gvcf_father=lambda wildcards: expand(
            rules.short_call_deeptrio.output.gvcf_father,
            sample=FAMILY_REPRESENTATIVE_CHILD[wildcards.family],
            allow_missing=True,
        ),
    output:
        vcfgz=DIR_PROC.joinpath(
            "15-genotype",
            "family_joint",
            "{family}_{read_type}.{aligner}-glnexus.{ref}.{chrom}.vcf.gz",
        ),
    log:
        DIR_LOG.joinpath(
            "15-genotype",
            "family_joint",
            "{family}_{read_type}.{aligner}-glnexus.{ref}.{chrom}.log",
        ),
    container:
        f"{CONTAINER_STORE}/{config['glnexus']}"
    threads: CPU_LOW
    params:
        glnexus_preset=config["glnexus_preset"],
    shell:
        "glnexus_cli --config {params.glnexus_preset} "
        "--threads {threads} "
        "{input.gvcfs_children} {input.gvcf_mother} {input.gvcf_father} "
        " | bcftools view -Oz -o {output.vcfgz} "
        " &> {log}"


if SAMPLE_PAIRS is not None:

    rule run_all_merge_genotypes:
        input:
            vcf=expand(
                rules.concat_region_sample_genotypes.output.vcf,
                read_type=["hifi"],
                ref=["t2tv2"],
                panel=["hgsvc3hprc"],
                allele_repr=["malc", "balc"],
            ),
            tbi=expand(
                rules.concat_region_sample_genotypes.output.tbi,
                read_type=["hifi"],
                ref=["t2tv2"],
                panel=["hgsvc3hprc"],
                allele_repr=["malc", "balc"],
            ),


if config["variant_calling_mode"] == "trio":
    TRIO_JOINT_OUTPUT = expand(
        rules.glnexus_trio_joint.output.vcfgz,
        sample=SINGLE_CHILD_FAMILIES,
        read_type=["hifi"],
        aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
        ref=USE_REF_GENOMES,
        chrom=CHROMOSOMES,
    )
    DUO_JOINT_OUTPUT = expand(
        rules.glnexus_duo_joint.output.vcfgz,
        sample=DUO_CHILDREN,
        read_type=["hifi"],
        aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
        ref=USE_REF_GENOMES,
        chrom=CHROMOSOMES,
    )
    FAMILY_JOINT_OUTPUT = expand(
        rules.glnexus_family_joint.output.vcfgz,
        family=MULTI_CHILD_FAMILIES,
        read_type=["hifi"],
        aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
        ref=USE_REF_GENOMES,
        chrom=CHROMOSOMES,
    )
