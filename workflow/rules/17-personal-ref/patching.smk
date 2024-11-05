
rule generate_consensus_sequence:
    """The output FASTA has adapted headers
    indicating that the genome is a PRG.
    The chain file needs to be adapted in a postprocessing
    step if needed, though.
    """
    input:
        ref_genome = lambda wildcards: REF_GENOMES[wildcards.ref],
        vcf = rules.concat_phased_chrom_vcfs.output.vcf,
        tbi = rules.concat_phased_chrom_vcfs.output.tbi
    output:
        fasta = DIR_PROC.joinpath(
            "17-personal-ref", "patching",
            "{sample}_{read_type}_{ref}_{panel}.H{hap}.fasta.gz"
        ),
        fai = DIR_PROC.joinpath(
            "17-personal-ref", "patching",
            "{sample}_{read_type}_{ref}_{panel}.H{hap}.fasta.gz.fai"
        ),
        chain = DIR_PROC.joinpath(
            "17-personal-ref", "patching",
            "{sample}_{read_type}_{ref}_{panel}.H{hap}.{ref}-to-prg.chain"
        )
    log:
        DIR_LOG.joinpath(
            "17-personal-ref", "patching",
            "{sample}_{read_type}_{ref}_{panel}.H{hap}.bcftools.log"
        )
    benchmark:
        DIR_RSRC.joinpath(
            "17-personal-ref", "patching",
            "{sample}_{read_type}_{ref}_{panel}.H{hap}.bcftools.rsrc"
        )
    wildcard_constraints:
        hap="(1|2)"
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 24576 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    params:
        script=find_script("fix_prg_header")
    shell:
        "bcftools consensus --samples {wildcards.sample} --haplotype {wildcards.hap} --exclude 'FMT/GQ<20' "
        "--fasta-ref {input.ref_genome} --chain {output.chain} {input.vcf} 2> {log}"
            " | "
        "{params.script} --sample {wildcards.sample} --haplotype {wildcards.hap}"
            " | "
        "bgzip > {output.fasta}"
            " && "
        "samtools faidx {output.fasta}"


rule combine_consensus_haplotypes:
    input:
        haps = expand(
            rules.generate_consensus_sequence.output.fasta,
            hap=[1,2],
            allow_missing=True
        )
    output:
        fasta = DIR_RES.joinpath(
            "personal_reference",
            "{sample}_{read_type}_{ref}_{panel}.wg.fasta.gz"
        ),
        fai = DIR_RES.joinpath(
            "personal_reference",
            "{sample}_{read_type}_{ref}_{panel}.wg.fasta.gz.fai"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    shell:
        "zcat {input.haps} | bgzip > {output.fasta}"
            " && "
        "samtools faidx {output.fasta}"


rule decompress_prg_fasta_file:
    """This rule only exists to accommodate
    tools such as pbsv that cannot deal with
    modern file formats such as gzipped FASTA
    files ... just overhead ...
    """
    input:
        fagz = load_prg_variant_fasta
    output:
        fasta = temp(DIR_PROC.joinpath(
            "temp", "prg_plain",
            "{sample}_{read_type}_{ref}_{panel}.{prg_variant}.fasta"
        )),
        fai = temp(DIR_PROC.joinpath(
            "temp", "prg_plain",
            "{sample}_{read_type}_{ref}_{panel}.{prg_variant}.fasta.fai"
        ))
    wildcard_constraints:
        prg_variant="(prg|prg1|prg2)"
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    shell:
        "pigz -c -d -p {threads} {input.fagz} > {output.fasta}"
            " && "
        "samtools faidx {output.fasta}"


rule run_all_generate_consensus:
    input:
        fasta = expand(
            rules.combine_consensus_haplotypes.output.fasta,
            sample=CONTROL_SAMPLES,
            read_type=["hifi"],
            ref=["t2tv2"],
            panel=["hgsvc3hprc"],
            hap=[1,2]
        )
