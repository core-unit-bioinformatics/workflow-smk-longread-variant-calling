
rule generate_consensus_sequence:
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
    shell:
        "bcftools consensus --samples {wildcards.sample} --haplotype {wildcards.hap} --exclude 'FMT/GQ<20' "
        "--fasta-ref {input.ref_genome} --chain {output.chain} {input.vcf} 2> {log}"
            " | "
        "bgzip > {output.fasta}"
            " && "
        "samtools faidx {output.fasta}"


rule run_all_generate_consensus:
    input:
        fasta = expand(
            rules.generate_consensus_sequence.output.fasta,
            sample=CONTROL_SAMPLES,
            read_type=["hifi"],
            ref=["t2tv2"],
            panel=["hgsvc3hprc"],
            hap=[1,2]
        )
