rule short_call_deepvariant_hifi:
    input:
        ref = lambda wildcards: REF_GENOMES[wildcards.ref],
        ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")],
        bam = rules.split_merged_alignments.output.main,
        bai = rules.split_merged_alignments.output.main_bai
    output:
        vcfgz = DIR_PROC.joinpath(
            "30-callshort", "{sample}_{read_type}.{aligner}-deepvar.{ref}.{chrom}.vcf.gz"
        )
    benchmark:
        DIR_RSRC.joinpath(
            "30-callshort", "{sample}_{read_type}.{aligner}-deepvar.{ref}.{chrom}.rsrc"
        )
    log:
        DIR_LOG.joinpath(
            "30-callshort", "{sample}_{read_type}.{aligner}-deepvar.{ref}.{chrom}.log"
        )
    container:
        f"{CONTAINER_STORE}/{config['deepvariant']}"
    threads: CPU_LOW
    resources:
        mem_mb = lambda wildcards, attempt: 16384 + 8192 * attempt,
        time_hrs = lambda wildcards, attempt: attempt**2,
        arch=":arch=skylake"  # docker default built with AVX512
    params:
        tempdir = lambda wildcards: DIR_PROC.joinpath(
            "temp", "deepvariant", wildcards.ref,
            wildcards.sample, wildcards.aligner, wildcards.chrom
        ),
        model = lambda wildcards: config["deepvariant_models"][wildcards.read_type]
    shell:
        "rm -rf {params.tempdir}"
            " && "
        "mkdir -p {params.tempdir}"
            " && "
        "/opt/deepvariant/bin/run_deepvariant --model_type {params.model} "
        "--ref {input.ref} --reads {input.bam} --num_shards {threads} "
        "--output_vcf {output.vcfgz} --regions {wildcards.chrom} "
        "--noruntime_report --novcf_stats_report "
        "--intermediate_results_dir {params.tempdir} &> {log}"
            " ; "
        "rm -rfd {params.tempdir}"


rule short_call_deeptrio:
    input:
        ref = lambda wildcards: REF_GENOMES[wildcards.ref],
        ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")],
        child_bam = rules.split_merged_alignments.output.main,
        child_bai = rules.split_merged_alignments.output.main_bai,
        mother_bam = lambda wildcards: expand(
            rules.split_merged_alignments.output.main,
            sample=MATERNAL_ID_MAP[wildcards.sample],
            allow_missing=True
        ),
        mother_bai = lambda wildcards: expand(
            rules.split_merged_alignments.output.main_bai,
            sample=MATERNAL_ID_MAP[wildcards.sample],
            allow_missing=True
        ),
        father_bam = lambda wildcards: expand(
            rules.split_merged_alignments.output.main,
            sample=PATERNAL_ID_MAP[wildcards.sample],
            allow_missing=True
        ),
        father_bai = lambda wildcards: expand(
            rules.split_merged_alignments.output.main_bai,
            sample=PATERNAL_ID_MAP[wildcards.sample],
            allow_missing=True
        )
    output:
        gvcf_child  = DIR_PROC.joinpath(
            "30-callshort", "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.child.{ref}.{chrom}.g.vcf.gz"
        ),
        gvcf_mother = DIR_PROC.joinpath(
            "30-callshort", "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.mother.{ref}.{chrom}.g.vcf.gz"
        ),
        gvcf_father = DIR_PROC.joinpath(
            "30-callshort", "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.father.{ref}.{chrom}.g.vcf.gz"
        ),
        vcf_child  = DIR_PROC.joinpath(
            "30-callshort", "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.child.{ref}.{chrom}.vcf.gz"
        ),
        vcf_mother = DIR_PROC.joinpath(
            "30-callshort", "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.mother.{ref}.{chrom}.vcf.gz"
        ),
        vcf_father = DIR_PROC.joinpath(
            "30-callshort", "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.father.{ref}.{chrom}.vcf.gz"
        )
    log:
        DIR_LOG.joinpath(
            "30-callshort", "trio",
            "{sample}_{read_type}.{aligner}-deeptrio.{ref}.{chrom}.log"
        )
    container:
        f"{CONTAINER_STORE}/{config['deeptrio']}"
    threads: CPU_LOW
    resources:
        mem_mb = lambda wildcards, attempt: 16384 + 8192 * attempt,
        time_hrs = lambda wildcards, attempt: attempt**2,
        arch=":arch=skylake"
    params:
        tempdir = lambda wildcards: DIR_PROC.joinpath(
            "temp", "deeptrio", wildcards.ref,
            wildcards.sample, wildcards.read_type, wildcards.aligner, wildcards.chrom
        ),
        model = lambda wildcards: config["deeptrio_models"][wildcards.read_type],
        child_name = lambda wildcards: wildcards.sample,
        mother_name = lambda wildcards: MATERNAL_ID_MAP[wildcards.sample],
        father_name = lambda wildcards: PATERNAL_ID_MAP[wildcards.sample]
    shell:
        "rm -rf {params.tempdir}"
        " && "
        "mkdir -p {params.tempdir}"
        " && "
        "/opt/deepvariant/bin/deeptrio/run_deeptrio "
        "--model_type {params.model} "
        "--ref {input.ref} "
        "--reads_child {input.child_bam} "
        "--reads_parent1 {input.mother_bam} "
        "--reads_parent2 {input.father_bam} "
        "--output_vcf_child {output.vcf_child} "
        "--output_vcf_parent1 {output.vcf_mother} "
        "--output_vcf_parent2 {output.vcf_father} "
        "--output_gvcf_child {output.gvcf_child} "
        "--output_gvcf_parent1 {output.gvcf_mother} "
        "--output_gvcf_parent2 {output.gvcf_father} "
        "--sample_name_child {params.child_name} "
        "--sample_name_parent1 {params.mother_name} "
        "--sample_name_parent2 {params.father_name} "
        "--regions {wildcards.chrom} "
        "--num_shards {threads} "
        "--intermediate_results_dir {params.tempdir} "
        "&> {log}"
        " ; "
        "rm -rfd {params.tempdir}"

rule run_deepvariant_hifi_calling:
    """TODO - the way the chromosomes
    are loaded here is not yet compatible
    with a personalized reference genome.
    """
    input:
        vcfs = expand(
            rules.short_call_deepvariant_hifi.output.vcfgz,
            sample=HIFI_SAMPLES,
            read_type=["hifi"],
            aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
            ref=USE_REF_GENOMES,
            chrom=CHROMOSOMES
        )

if config["variant_calling_mode"] == "trio":
    rule run_deeptrio_hifi_calling:
        input:
            gvcf_child = expand(
                rules.short_call_deeptrio.output.gvcf_child,
                sample=TRIO_CHILDREN,
                read_type=["hifi"],
                aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
                ref=USE_REF_GENOMES,
                chrom=CHROMOSOMES
            ),
            gvcf_mother = expand(
                rules.short_call_deeptrio.output.gvcf_mother,
                sample=TRIO_CHILDREN,
                read_type=["hifi"],
                aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
                ref=USE_REF_GENOMES,
                chrom=CHROMOSOMES
            ),
            gvcf_father = expand(
                rules.short_call_deeptrio.output.gvcf_father,
                sample=TRIO_CHILDREN,
                read_type=["hifi"],
                aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
                ref=USE_REF_GENOMES,
                chrom=CHROMOSOMES
            ),
            vcf_child = expand(
                rules.short_call_deeptrio.output.vcf_child,
                sample=TRIO_CHILDREN,
                read_type=["hifi"],
                aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
                ref=USE_REF_GENOMES,
                chrom=CHROMOSOMES
            ),
            vcf_mother = expand(
                rules.short_call_deeptrio.output.vcf_mother,
                sample=TRIO_CHILDREN,
                read_type=["hifi"],
                aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
                ref=USE_REF_GENOMES,
                chrom=CHROMOSOMES
            ),
            vcf_father = expand(
                rules.short_call_deeptrio.output.vcf_father,
                sample=TRIO_CHILDREN,
                read_type=["hifi"],
                aligner=ALIGNER_FOR_CALLER[("deepvar", "hifi")],
                ref=USE_REF_GENOMES,
                chrom=CHROMOSOMES
            )
